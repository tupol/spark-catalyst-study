package study.catalyst

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GenerateSafeProjection}
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.types.DoubleType

import scala.util.Try


/**
  * Look MA! NO HANDS!
  * Sorry, I mean no SparkContext, no SqlContext... just Catalyst!
  */
object SimpleExpressionComplexExample3 extends App {

  // Code generation context
  implicit val codegenContext = new CodegenContext()

  // Define a simple attribute for out "variable" x
  val x = AttributeReference("x", DoubleType)()

  println(s"The x attribute is: $x")

  // Set the order the attributes are presented in the input
  val attributes = AttributeSeq(Seq(x))

  // Define a simple add expression that would look something like `1 + ( x + 1 )`
  // This rewritten form shows the limitations of hte current optimizer... does not know about associativity or commutativity
  val addExpressionRaw = Add( x, Literal(0.0) )

  println(s"The raw expression is: $addExpressionRaw")
  // returns `UDF((1.0 + (x#0 + 1.0)))`

  // Bind the AttributeReferences into BoundReferences
  val expressionBound = BindReferences.bindReference(addExpressionRaw, attributes)

  println(s"The bound expression is: $expressionBound")
  // returns `UDF((1.0 + (input[0, double, true] + 1.0)))`

  // Name the expression
  val myExpression = Alias(expressionBound, "add0")()
  println(s"The named expression is: $myExpression")
  // returns `UDF((1.0 + (input[0, double, true] + 1.0))) AS myUDF#1`

  // Define a local relation or a table structure that contains a list of attributes (columns)
  val localRelation: LocalRelation = LocalRelation(x)

  println(s"The local relation is: $localRelation")
  // returns `LocalRelation <empty>, [x#0]`

  // Define a projection by applying a sequence of expression to the localRelation
  val projection = Project(Seq(myExpression), localRelation)

  println(s"The projection is: \n$projection")


  println(s"The filtered plan tree is: \n$projection")
  // returns
  //  'Filter (UDF((input[0, double, true] + 3.0)) AS myUDF#1 >= 10000)
  //  +- LocalRelation <empty>, [x#0]

  val analyzedPlan: LogicalPlan = SimpleAnalyzer.execute(projection)

  println(s"The analyzed plan tree is: \n$analyzedPlan")
  //returns
  //  Filter (if (isnull((input[0, double, true] + 3.0))) null else UDF((input[0, double, true] + 3.0)) >= cast(10000 as double))
  //  +- LocalRelation <empty>, [x#0]

  val optimizedPlan = SimpleTestOptimizer.execute(analyzedPlan)

  val generatedByteCode: Projection = GenerateSafeProjection.generate(analyzedPlan.expressions)

  println(generatedByteCode.apply(InternalRow(0.0)))
  println(generatedByteCode(InternalRow(1.0)))
  println(generatedByteCode(InternalRow(2.0)))
  println(Try(generatedByteCode(InternalRow(1))))
  println(Try(generatedByteCode(InternalRow("1.0"))))

}
