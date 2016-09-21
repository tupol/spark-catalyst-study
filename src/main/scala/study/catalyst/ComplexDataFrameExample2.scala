package study.catalyst

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, GeneratedClass}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object ComplexDataFrameExample2 extends App {


  val myUDF1: UserDefinedFunction = udf((in: String) => s"(${math.exp(in.length / 2)} $in)")
  val myUDF2: UserDefinedFunction = udf((in: Int) => math.exp(in / 2))


  implicit val spark = SparkSession
    .builder()
    .appName("DFExample")
    .master("local")
    .getOrCreate()


    val people = createPeople(spark)
//    val actions = createActions(spark)


//    val dataFrame1: DataFrame = people.select(people("id"))

    println("---------------------------")
    println("- create dataFrame2")
    println("- - - - - - - - - - - - - -")

    val dataFrame2: DataFrame = people.select(
      people("id"),
      myUDF1(people("name")).as("new_name"),
      myUDF2(people("id")).as("new_id"),
      (people("id") + people("id") * people("id") * 3) as "new_col"
    ).where("id > 2")

    println("---------------------------")
    println("- printStuffAbout")
    println("- - - - - - - - - - - - - -")


    printStuffAbout(dataFrame2)



  def printStuffAbout(dataFrame: DataFrame): Unit = {


    implicit val codeGenContext = new CodegenContext()

    println("---------------------------")
    println("- dataFrame.show")
    println("- - - - - - - - - - - - - -")

    dataFrame.show

    println("---------------------------")
    println("- dataFrame.queryExecution.sparkPlan")
    println("- - - - - - - - - - - - - -")

    val execution = dataFrame.queryExecution
    val plan: SparkPlan = execution.executedPlan



    println("---------------------------")
    println("- WholeStageCodegenExec(InputAdapter(plan))")
    println("- - - - - - - - - - - - - -")

//    val wrappedPlan = WholeStageCodegenExec(InputAdapter(plan.children.head))
    val wrappedPlan: WholeStageCodegenExec = plan.asInstanceOf[WholeStageCodegenExec]

    println("---------------------------")
    println("- wrappedPlan.doCodeGen()")
    println("- - - - - - - - - - - - - -")

    val (cgContext, codeAndComment) = wrappedPlan.doCodeGen()

    println(plan.getClass.getName)

    println("---------------------------")
    println("- codeAndComment.body")
    println("- - - - - - - - - - - - - -")
    println(codeAndComment.body)
    println("---------------------------")
    println("- CodeGenerator.compile(codeAndComment)")
    println("- - - - - - - - - - - - - -")

    val newCode: GeneratedClass = CodeGenerator.compile(codeAndComment)


    printLP(wrappedPlan)

    println(getScalaUDFs(wrappedPlan))

    val params: Seq[Serializable] = new SQLMetric("x", 0) +: getScalaUDFs(wrappedPlan)

    val bri: BufferedRowIterator = newCode.generate(params.toArray).asInstanceOf[BufferedRowIterator]


    println("---------------------------")
    println("- bri.init")
    println("- - - - - - - - - - - - - -")

    bri.init(0, Array(Seq(InternalRow(1, "Barbie")).iterator))

    println("---------------------------")
    println(bri.hasNext)
    if(bri.hasNext)
      println(bri.next())
    println("---------------------------")

  }

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

}

