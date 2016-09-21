package study.catalyst

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, GeneratedClass}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object ComplexDataFrameExample1 extends App {


  val myUDF1: UserDefinedFunction = udf((in: String) => s"(${math.exp(in.length / 2)} $in)")
  val myUDF2: UserDefinedFunction = udf((in: Int) => math.exp(in / 2))


  implicit val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .master("local")
    .getOrCreate()

    val people = createPeople(spark)

    val sampleDF: DataFrame = people.select(
      people("id"),
      myUDF1(people("name")).as("new_name"),
      myUDF2(people("id")).as("new_id"),
      (people("id") + people("id") * people("id") * 3) as "new_col"
    ).where("id > 2")


    printStuffAbout(sampleDF)


  def printStuffAbout(dataFrame: DataFrame): Unit = {

    dataFrame.show

    val execution = dataFrame.queryExecution
    val plan: SparkPlan = execution.executedPlan

    val wrappedPlan: WholeStageCodegenExec = plan.asInstanceOf[WholeStageCodegenExec]

    val (cgContext, codeAndComment) = wrappedPlan.doCodeGen()

    val newCode: GeneratedClass = CodeGenerator.compile(codeAndComment)


    val params: Seq[Serializable] = new SQLMetric("x", 0) +: getScalaUDFs(wrappedPlan)

    val bri: BufferedRowIterator = newCode.generate(params.toArray).asInstanceOf[BufferedRowIterator]


    bri.init(0, Array(Seq(InternalRow(1, "Barbie")).iterator))

    println("---------------------------")
    println(bri.hasNext)
    if(bri.hasNext)
      println(bri.next())
    println("---------------------------")

  }


}

