package org.apache.spark.sql.catalyst.expressions.codegen

import java.io.ByteArrayInputStream
import java.util.{Map => JavaMap}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.{ParentClassLoader, Utils}
import org.codehaus.janino.util.ClassFile
import org.codehaus.janino.{ByteArrayClassLoader, ClassBodyEvaluator, SimpleCompiler}

import scala.collection.JavaConverters._


/**
  * A wrapper for generated class, defines a `generate` method so that we can pass extra objects
  * into generated class.
  */
abstract class CustomGeneratedClass extends Serializable {
  def generate(references: Array[Any]): Any
}

/**
  *
  */
object CustomCodeGenerator extends Logging {
  /**
    * Compile the Java source code into a Java class, using Janino.
    */
  def compile(code: CodeAndComment): CustomGeneratedClass = {
    cache.get(code)
  }

  /**
    * Compile the Java source code into a Java class, using Janino.
    */
  private[this] def doCompile(code: CodeAndComment): CustomGeneratedClass = {
    val evaluator = new ClassBodyEvaluator()

    // A special classloader used to wrap the actual parent classloader of
    // [[org.codehaus.janino.ClassBodyEvaluator]] (see CodeGenerator.doCompile). This classloader
    // does not throw a ClassNotFoundException with a cause set (i.e. exception.getCause returns
    // a null). This classloader is needed because janino will throw the exception directly if
    // the parent classloader throws a ClassNotFoundException with cause set instead of trying to
    // find other possible classes (see org.codehaus.janinoClassLoaderIClassLoader's
    // findIClass method). Please also see https://issues.apache.org/jira/browse/SPARK-15622 and
    // https://issues.apache.org/jira/browse/SPARK-11636.
    val parentClassLoader = new ParentClassLoader(Utils.getContextOrSparkClassLoader)
    evaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName("CustomGeneratedClass")
    evaluator.setDefaultImports(Array(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[MutableRow].getName,
      classOf[Expression].getName
    ))
    evaluator.setExtendedClass(classOf[CustomGeneratedClass])

    lazy val formatted = CodeFormatter.format(code)

    logDebug({
      // Only add extra debugging info to byte code when we are going to print the source code.
      evaluator.setDebuggingInformation(true, true, false)
      s"\n$formatted"
    })

    try {
      evaluator.cook("generated.java", code.body)
      recordCompilationStats(evaluator)
    } catch {
      case e: Exception =>
        val msg = s"failed to compile: $e\n$formatted"
        logError(msg, e)
        throw new Exception(msg, e)
    }
    evaluator.getClazz().newInstance().asInstanceOf[CustomGeneratedClass]
  }

  /**
    * Records the generated class and method bytecode sizes by inspecting janino private fields.
    */
  private def recordCompilationStats(evaluator: ClassBodyEvaluator): Unit = {
    // First retrieve the generated classes.
    val classes = {
      val resultField = classOf[SimpleCompiler].getDeclaredField("result")
      resultField.setAccessible(true)
      val loader = resultField.get(evaluator).asInstanceOf[ByteArrayClassLoader]
      val classesField = loader.getClass.getDeclaredField("classes")
      classesField.setAccessible(true)
      classesField.get(loader).asInstanceOf[JavaMap[String, Array[Byte]]].asScala
    }

    // Then walk the classes to get at the method bytecode.
    val codeAttr = Utils.classForName("org.codehaus.janino.util.ClassFile$CodeAttribute")
    val codeAttrField = codeAttr.getDeclaredField("code")
    codeAttrField.setAccessible(true)
    classes.foreach { case (_, classBytes) =>
      CodegenMetrics.METRIC_GENERATED_CLASS_BYTECODE_SIZE.update(classBytes.length)
      val cf = new ClassFile(new ByteArrayInputStream(classBytes))
      cf.methodInfos.asScala.foreach { method =>
        method.getAttributes().foreach { a =>
          if (a.getClass.getName == codeAttr.getName) {
            CodegenMetrics.METRIC_GENERATED_METHOD_BYTECODE_SIZE.update(
              codeAttrField.get(a).asInstanceOf[Array[Byte]].length)
          }
        }
      }
    }
  }

  /**
    * A cache of generated classes.
    *
    * From the Guava Docs: A Cache is similar to ConcurrentMap, but not quite the same. The most
    * fundamental difference is that a ConcurrentMap persists all elements that are added to it until
    * they are explicitly removed. A Cache on the other hand is generally configured to evict entries
    * automatically, in order to constrain its memory footprint.  Note that this cache does not use
    * weak keys/values and thus does not respond to memory pressure.
    */
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[CodeAndComment, CustomGeneratedClass]() {
        override def load(code: CodeAndComment): CustomGeneratedClass = {
          val startTime = System.nanoTime()
          val result = doCompile(code)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          CodegenMetrics.METRIC_SOURCE_CODE_SIZE.update(code.body.length)
          CodegenMetrics.METRIC_COMPILATION_TIME.update(timeMs.toLong)
          logInfo(s"Code generated in $timeMs ms")
          result
        }
      })
}
