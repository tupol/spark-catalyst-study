package study.catalyst.kmeans

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.custom.CustomGenerateSafeProjection
import org.apache.spark.unsafe.types.UTF8String
import study.catalyst._
import study.catalyst.kmeans.KMeansPipelineBuilder._

/**
  * This class shows a primitive way of serializing data required for code generation and some metadata
  */
object KMeansGenExample {

  val CODEGEN_PARAMETERS_FILE =  "/tmp/CodeGenParameters.ser"

  def main(args: Array[String]) = {

    val inputDS: DataFrame = spark.createDataFrame(sampleClicks)
    val transformedDS = pipeline.transform(inputDS)

    transformedDS.show

    // Get the work plan that we are going to work with
    val workPlan = transformedDS.queryExecution.optimizedPlan

    // Create a binder that will bind references in all the expressions
    val binder: PartialFunction[Expression, Expression] = {
      case (expression: Expression) => BindReferences.bindReference(expression, workPlan.allAttributes)
    }

    // Bind all the expressions in our plan
    val resolvedExpressions: Seq[Alias] = workPlan.expressions.
      map(binder).zipWithIndex.map { case (ex, id) => Alias(ex, s"$id")() }

    // Generate the building blocks of our custom projection
    val (code, references) = CustomGenerateSafeProjection.generateCodeAndRef(resolvedExpressions)

    // Set some metadata, very useful to figure out the inputs and outputs
    val metadata = Metadata(workPlan.inputSet.toSeq, workPlan.output)

    val genParams = CodeGenParameters(code, references, metadata)


    // Serialize the CodeGenParameters, so we can also try it somewhere else (like in KMeansClientExample)
    writeObject(genParams, CODEGEN_PARAMETERS_FILE)

    // Load the previously serialized parameters to do a primitive serialization test
    // This is primitive as we are in the same class loader... jvm...
    val loadedParams: CodeGenParameters = readObject[CodeGenParameters](CODEGEN_PARAMETERS_FILE).get

    // Generate a projection out of the loaded generation parameters
    val generatedProjection: Projection = CustomGenerateSafeProjection.create(loadedParams.code, loadedParams.references)

    println("----------------------------------------------")
    // Print the data used to train the model, just for kicks
    sampleClicks.foreach(println)
    println("----------------------------------------------")
    // We need to hack the data a little, by transforming the Strings into UTF8Strings
    // Also we are converting the input case class into InternalRows
    val inRows = sampleClicks.map(c => InternalRow(c.productIterator.collect{case s: String => UTF8String.fromString(s)}.toSeq: _*))
    // Let's see how the InternalRows look like
    inRows.foreach(println)
    println("----------------------------------------------")
    // Let's see how the results look like
    inRows.foreach(r => println(generatedProjection(r)))
    println("----------------------------------------------")

  }

}





