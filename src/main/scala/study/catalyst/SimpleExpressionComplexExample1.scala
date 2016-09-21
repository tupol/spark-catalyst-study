package study.catalyst

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CustomGenerateSafeProjection}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, PushPredicateThroughJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.types.DoubleType

import scala.util.Try


/**
  * Look MA! NO HANDS!
  * Sorry, I mean no SparkContext, no SqlContext... just Catalyst!
  *
  * Steps:
  * 1.  create an attribute
  * 2.  create a simple add expression that looks like `x + ( 1 + 2 )`
  * 3.  bind the expression to solve the attribute references
  * 4.  create a logical plan
  * 5.  project our expression on the logical plan
  * 6.  optimize the projected plan using constant folding
  * 7.  generate code from the previously optimized plan
  * -
  * 8.  apply a filter to the plan using a `>=` expression
  * 9.  optimize the plan using predicate push-down (need to work on this a bit more, since it has no effect in this case)
  * 10. generate new code
  */
object SimpleExpressionComplexExample1 extends App {

  // Code generation context
  implicit val codegenContext = new CodegenContext()

  // Define a simple attribute for out "variable" x
  val x = AttributeReference("x", DoubleType)()

  println(s"The x attribute is: $x")

  //Define a simple add expression that would look something like `x + ( 1 + 2 )`
  val addExpressionRaw = Add( x, Add( Literal(1.0), Literal(2.0) ) )

  println(s"The raw expression is: $addExpressionRaw")
  // returns `(x#0 + (1.0 + 2.0))`

  // Set the order the attributes are presented in the input
  val attributes = AttributeSeq(Seq(x))

  // Bind the AttributeReferences into BoundReferences
  val addExpressionBound = BindReferences.bindReference(addExpressionRaw, attributes)

  println(s"The bound expression is: $addExpressionBound")
  // returns `(input[0, double, true] + (1.0 + 2.0))`

  // Name the expression
  val addExpression = Alias(addExpressionBound, "add")()
  println(s"The named expression is: $addExpression")
  // returns `(input[0, double, true] + (1.0 + 2.0)) AS add#1`

  // Define a local relation or a table structure that contains a list of attributes (columns)
  val localRelation: LocalRelation = LocalRelation(x)

  println(s"The local relation is: $localRelation")
  // returns `LocalRelation <empty>, [x#0]`

  // Define a projection by applying a sequence of expression to the localRelation
  val projection = Project(Seq(addExpression), localRelation)

  println(s"The projection is: \n$projection")
  // returns `Project [(input[0, double, true] + (1.0 + 2.0)) AS add#1]
  //          +- LocalRelation <empty>, [x#0]`

  val optimisedExpression = ConstantFolding(projection).expressions(0)

  println(s"The optimised expression is: $optimisedExpression")
  // returns `LocalRelation <empty>, [x#0]`

  println("The generated code for the optimised add expression is:")
  println("------------------------")
  println(generateCode(optimisedExpression).code)
  println("------------------------")
  // returns
  //      boolean isNull = true;
  //      double value = -1.0;
  //
  //      boolean isNull1 = i.isNullAt(0);
  //      double value1 = isNull1 ? -1.0 : (i.getDouble(0));
  //      if (!isNull1) {
  //        isNull = false; // resultCode could change nullability.
  //        value = value1 + 3.0D;
  //      }

  // Filter the current plan
  val filterOptimisedExpression= GreaterThanOrEqual(optimisedExpression, Literal(10))
  val filteredPlan: Filter = Filter(filterOptimisedExpression,localRelation)

  println(s"The filtered plan tree is: \n$filteredPlan")

  val analyzedPlan: LogicalPlan = SimpleAnalyzer.execute(filteredPlan)

  println(s"The analyzed plan tree is: \n$analyzedPlan")

  val filterPushOptimizedPlan: LogicalPlan = PushPredicateThroughJoin(analyzedPlan)

  println(s"The optimized plan is: \n${filterPushOptimizedPlan}")

  println("The generated code for the PushPredicateThroughJoin optimised plan is:")
  println("------------------------")
  println(generateCode(filterPushOptimizedPlan.expressions(0)).code)
  println("------------------------")
  // returns
  //      boolean isNull3 = true;
  //      boolean value3 = false;
  //
  //      boolean isNull4 = true;
  //      double value4 = -1.0;
  //
  //      boolean isNull5 = i.isNullAt(0);
  //      double value5 = isNull5 ? -1.0 : (i.getDouble(0));
  //      if (!isNull5) {
  //        isNull4 = false; // resultCode could change nullability.
  //        value4 = value5 + 3.0D;
  //      }
  //      if (!isNull4) {
  //        boolean isNull7 = false;
  //        double value7 = -1.0;
  //        if (!false) {
  //          value7 = (double) 10;
  //        }
  //
  //        isNull3 = false; // resultCode could change nullability.
  //        value3 = org.apache.spark.util.Utils.nanSafeCompareDoubles(value4, value7) >= 0;
  //
  //      }


  // Serialize our plan to disk
  writeObject(filterPushOptimizedPlan, "/tmp/myPlan.bin")
  // Load the plan from disk
  val loadedPlan: LogicalPlan = readObject[LogicalPlan]("/tmp/myPlan.bin").get

  // Generate byte code from the deserialised plan
  val genFun: Projection = CustomGenerateSafeProjection.generate(loadedPlan.expressions)

  println(genFun.apply(InternalRow(0.0)))
  println(genFun(InternalRow(6.0)))
  println(genFun(InternalRow(7.0)))
  println(Try(genFun(InternalRow(1))))
  println(Try(genFun(InternalRow("1.0"))))

  writeObject(genFun, "/tmp/CustomGeneratedClass.class")
  writeObject(genFun, "target/scala-2.11/classes/CustomGeneratedClass.class")
  val loadedFun: Projection = readObject[Projection]("/tmp/CustomGeneratedClass.class").get

  println(loadedFun(InternalRow(6.0)))
  println(loadedFun(InternalRow(7.0)))

}
