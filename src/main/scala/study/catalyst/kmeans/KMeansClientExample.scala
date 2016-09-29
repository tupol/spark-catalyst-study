package study.catalyst.kmeans

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.custom.CustomGenerateSafeProjection
import org.apache.spark.unsafe.types.UTF8String
import study.catalyst._
import study.catalyst.data._

/**
  *
  */
object KMeansClientExample {

  val CODEGEN_PARAMETERS_FILE = KMeansGenExample.CODEGEN_PARAMETERS_FILE

  def main(args: Array[String]) = {

    val cgParams: CodeGenParameters = readObject[CodeGenParameters](CODEGEN_PARAMETERS_FILE).get

    val generatedProjection: Projection = CustomGenerateSafeProjection.create(cgParams.code, cgParams.references)

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




