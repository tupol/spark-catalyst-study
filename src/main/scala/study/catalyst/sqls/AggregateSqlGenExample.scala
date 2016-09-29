package study.catalyst.sqls

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.custom.CustomGenerateSafeProjection
import org.apache.spark.unsafe.types.UTF8String
import study.catalyst._
import study.catalyst.data._

/**
  * This class shows a primitive way of serializing data required for code generation and some metadata
  */
object AggregateSqlGenExample {

  val CODEGEN_PARAMETERS_FILE =  "/tmp/CodeGenParametersAggr.ser"

  def main(args: Array[String]) = {

    val inputDS: DataFrame = spark.createDataFrame(sampleClicks)

    inputDS.createOrReplaceTempView("clicks")

    val transformedDS = spark.sql("""
        SELECT
               sourceCountryAndCity, count(sourceCountryAndCity)
        FROM  clicks
        WHERE `timestamp` IS NOT NULL
        GROUP BY sourceCountryAndCity
      """
    )

    transformedDS.show

    // Get the work plan that we are going to work with
    val workPlan = transformedDS.queryExecution.optimizedPlan

    println("----------------------------------------------")
    println(workPlan.treeString(true))
    println("----------------------------------------------")

    // Generate the building blocks of our custom projection
    val (code, references) = CustomGenerateSafeProjection.generateCodeAndRef(workPlan)

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





