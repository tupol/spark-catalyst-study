package study

import java.io._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.ProjectExec

import scala.util.Try

/**
  * Some utility functions
  */
package object catalyst {


  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .master("local")
    .getOrCreate()

  /**
    * Utility function for code generation of a single expression
    *
    * @param expression
    * @param codegenContext
    * @return
    */
  def generateCode(expression: Expression)(implicit codegenContext: CodegenContext)= {
    codegenContext.generateExpressions(Seq(expression)).head
  }


  /**
    * Read a serialised object from a file
    *
    * @param file
    * @tparam T
    * @return
    */
  def readObject[T](file: File): Try[T] =
    Try {
      var fileIn: FileInputStream = null
      var in: ObjectInputStream = null
      try {
        fileIn = new FileInputStream(file)
        in = new ObjectInputStream(fileIn)
        in.readObject().asInstanceOf[T]
      } finally {
        in.close()
        fileIn.close()
      }
//      SerializationUtils.deserialize[T](new FileInputStream(file))
    }

  /**
    * Read a serialised object from a file
    *
    * @param file
    * @tparam T
    * @return
    */
  def readObject[T](file: String): Try[T] = readObject[T](new File(file))


  /**
    * Write an object to a file
    *
    * @param file
    * @tparam T
    * @return
    */
  def writeObject[T](obj: T, file: File): Try[Unit] =
    Try {
      var fileOut: FileOutputStream = null
      var out: ObjectOutputStream = null
      try {
        fileOut = new FileOutputStream(file)
        out = new ObjectOutputStream(fileOut)
        out.writeObject(obj)
      } finally {
        out.close()
        fileOut.close()
      }
//      SerializationUtils.serialize(obj, new FileOutputStream(file))
    }



  /**
    * Write an string lines to a file
    *
    * @param file
    * @return
    */
  def writeLines(lines: Seq[String], file: String): Try[Unit] =
    Try {
      // Create a writer
      val writer = new PrintWriter(new BufferedOutputStream(new FileOutputStream(file)), true)
      //write each line
      lines.foreach(line => Try(writer.println(line)))
      // Close the streams
      Try(writer.close())
    }


  /**
    * Write an object to a file
 *
    * @param file
    * @tparam T
    * @return
    */
  def writeObject[T](obj: T, file: String): Try[Unit] = writeObject(obj, new File(file))


  /**
    * Extract a sequence of ScalaUDFs from a given TreeNode
 *
    * @param node
    * @param result
    * @return
    */
  def getScalaUDFs(node: TreeNode[_], result: Seq[ScalaUDF] = Seq()): Seq[ScalaUDF] = {
    val udfs = node match {
      case p: Project => p.projectList.flatMap(c => getScalaUDFs(c))
      case p: ProjectExec => p.projectList.flatMap(c => getScalaUDFs(c))
      case s: ScalaUDF => s :: Nil
      case _ => Nil
    }
    val childNodes = node.children.filter(_.isInstanceOf[TreeNode[_]]).map((_.asInstanceOf[TreeNode[_]]))
    udfs ++ childNodes.flatMap(c => getScalaUDFs(c)) ++ result
  }

  /**
    * Print some quasi random stuff about a TreeNode
 *
    * @param node
    * @param indentation
    */
  def printLP(node: TreeNode[_], indentation: String = "  "): Unit = {
    def indent(depth: Int) = (0 until depth).map(_ => indentation).mkString("")
    def printLP(node: TreeNode[_], depth: Int): Unit = {
      println(s"${indent(depth)}* ${node.getClass.getSimpleName}")
      node match {
        case p: Project => p.projectList.foreach(c => printLP(c, depth + 4))
        case p: ProjectExec => p.projectList.foreach(c => printLP(c, depth + 2))
        case s: ScalaUDF => println(s"${indent(depth + 2)}++ ${s.function}(${s.inputTypes}): ${s.dataType}")
        case a: AttributeReference => println(s"${indent(depth + 2)}++ ${a.name} ${a.dataType} ${a.metadata}")
        case a: Alias => println(s"${indent(depth + 2)}++ ${a.name} ${a.dataType} ${a.metadata} ")
        case x => println(s"${indent(depth + 2)}++ ${x.getClass.getSimpleName}")
      }

      node.children.filter(_.isInstanceOf[TreeNode[_]]).foreach(c => printLP(c.asInstanceOf[TreeNode[_]], depth + 1))
    }
    printLP(node, 0)
  }
  def describeUDF(udf: ScalaUDF, id: Option[Any] = None) = {
    println("----------------------------------------------")
    println(s"--- ScalaUDF Description ${if(id.isDefined) " : " + id.toString}")
    println(s"nodeName = ${udf.nodeName}")
    println(s"data type = ${udf.dataType}")
    println(s"deterministic = ${udf.deterministic}")
    println(s"userDefinedFunc = ${udf.userDefinedFunc}")
    println(s"children:")
    udf.children.foreach(c => {println(s"  - ${c.getClass}"); println(s"    - $c")})
    println(s"inputTypes:")
    udf.inputTypes.foreach(c => println(s"  - $c"))
    println(s"references:")
    udf.references.foreach(c => println(s"  - $c"))
    println(s"treeString:")
    println(udf.treeString(true))
    println("----------------------------------------------")
  }

}
