package study.catalyst

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, CodegenContext}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, PushPredicateThroughJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.types.DoubleType

import scala.util.Try


/**
  * Look MA! NO HANDS!
  * Sorry, I mean no SparkContext, no SqlContext... just Catalyst!
  */
object SimpleExpressionComplexExample2 extends App {

  // Code generation context
  implicit val codegenContext = new CodegenContext()

  // Define a simple attribute for out "variable" x
  val x = AttributeReference("x", DoubleType)()

  println(s"The x attribute is: $x")

  // Set the order the attributes are presented in the input
  val attributes = AttributeSeq(Seq(x))

  // Define a simple add expression that would look something like `1 + ( x + 1 )`
  // This rewritten form shows the limitations of hte current optimizer... does not know about associativity or commutativity
  val addExpressionRaw = Add( Literal(1.0), Add( x, Literal(1.0) ) )

  // Define a udf applied to the simple expression
  val udfExpression = ScalaUDF((x: Double) => x*x, DoubleType, Seq(addExpressionRaw), Seq(addExpressionRaw.dataType))

  println(s"The raw expression is: $udfExpression")
  // returns `UDF((1.0 + (x#0 + 1.0)))`

  // Bind the AttributeReferences into BoundReferences
  val expressionBound = BindReferences.bindReference(udfExpression, attributes)

  println(s"The bound expression is: $expressionBound")
  // returns `UDF((1.0 + (input[0, double, true] + 1.0)))`

  // Name the expression
  val myExpression = Alias(expressionBound, "myUDF")()
  println(s"The named expression is: $myExpression")
  // returns `UDF((1.0 + (input[0, double, true] + 1.0))) AS myUDF#1`

  // Define a local relation or a table structure that contains a list of attributes (columns)
  val localRelation: LocalRelation = LocalRelation(x)

  println(s"The local relation is: $localRelation")
  // returns `LocalRelation <empty>, [x#0]`

  // Define a projection by applying a sequence of expression to the localRelation
  val projection = Project(Seq(myExpression), localRelation)

  println(s"The projection is: \n$projection")

  val optimisedExpression = ConstantFolding(projection).expressions(0)

  println(s"The optimised expression is: $optimisedExpression")
  // returns `UDF((1.0 + (input[0, double, true] + 1.0))) AS myUDF#1`
  // Some work can be done on further optimizing arithmetic expressions

  println("The generated code for the optimised udf expression is:")
  println("------------------------")
  println(generateCode(optimisedExpression).code)
  println("------------------------")
  // returns
  //      boolean isNull1 = true;
  //      double value1 = -1.0;
  //
  //      boolean isNull2 = i.isNullAt(0);
  //      double value2 = isNull2 ? -1.0 : (i.getDouble(0));
  //      if (!isNull2) {
  //        isNull1 = false; // resultCode could change nullability.
  //        value1 = value2 + 3.0D;
  //      }
  //      Object arg = isNull1 ? null : converter.apply(value1);
  //      Double result = (Double)catalystConverter.apply(udf.apply(arg));
  //
  //      boolean isNull = result == null;
  //      double value = -1.0;
  //      if (!isNull) {
  //        value = result;
  //      }

  // Filter the current plan
  val filterOptimisedExpression= GreaterThanOrEqual(optimisedExpression, Literal(10))
  val filteredPlan: Filter = Filter(filterOptimisedExpression,localRelation)

  println(s"The filtered plan tree is: \n$filteredPlan")
  // returns
  //  'Filter (UDF((input[0, double, true] + 3.0)) AS myUDF#1 >= 10000)
  //  +- LocalRelation <empty>, [x#0]

  val analyzedPlan: LogicalPlan = SimpleAnalyzer.execute(filteredPlan)

  println(s"The analyzed plan tree is: \n$analyzedPlan")
  //returns
  //  Filter (if (isnull((input[0, double, true] + 3.0))) null else UDF((input[0, double, true] + 3.0)) >= cast(10000 as double))
  //  +- LocalRelation <empty>, [x#0]

  val filterPushOptimizedPlan: LogicalPlan = PushPredicateThroughJoin(analyzedPlan)

  println(s"The optimized plan is: \n${filterPushOptimizedPlan}")
  //returns
  //  Filter (if (isnull((input[0, double, true] + 3.0))) null else UDF((input[0, double, true] + 3.0)) >= cast(10000 as double))
  //  +- LocalRelation <empty>, [x#0]

  println("The generated code for the PushPredicateThroughJoin optimised plan is:")
  println("------------------------")
  println(generateCode(filterPushOptimizedPlan.expressions(0)).code)
  println("------------------------")
  // returns
  //      boolean isNull4 = true;
  //      boolean value4 = false;
  //
  //      boolean isNull7 = true;
  //      double value7 = -1.0;
  //
  //      boolean isNull8 = i.isNullAt(0);
  //      double value8 = isNull8 ? -1.0 : (i.getDouble(0));
  //      if (!isNull8) {
  //        isNull7 = false; // resultCode could change nullability.
  //        value7 = value8 + 3.0D;
  //      }
  //      boolean isNull5 = false;
  //      double value5 = -1.0;
  //      if (!false && isNull7) {
  //        final double value10 = -1.0;
  //        isNull5 = true;
  //        value5 = value10;
  //      } else {
  //        boolean isNull12 = true;
  //        double value12 = -1.0;
  //
  //        boolean isNull13 = i.isNullAt(0);
  //        double value13 = isNull13 ? -1.0 : (i.getDouble(0));
  //        if (!isNull13) {
  //          isNull12 = false; // resultCode could change nullability.
  //          value12 = value13 + 3.0D;
  //
  //        }
  //        Object arg1 = isNull12 ? null : converter1.apply(value12);
  //        Double result1 = (Double)catalystConverter1.apply(udf1.apply(arg1));
  //
  //        boolean isNull11 = result1 == null;
  //        double value11 = -1.0;
  //        if (!isNull11) {
  //          value11 = result1;
  //        }
  //        isNull5 = isNull11;
  //        value5 = value11;
  //      }
  //      if (!isNull5) {
  //        boolean isNull15 = false;
  //        double value15 = -1.0;
  //        if (!false) {
  //          value15 = (double) 10000;
  //        }
  //
  //        isNull4 = false; // resultCode could change nullability.
  //        value4 = org.apache.spark.util.Utils.nanSafeCompareDoubles(value5, value15) >= 0;
  //
  //      }

  val generatedByteCode: Projection = GenerateSafeProjection.generate(filterPushOptimizedPlan.expressions)

  println(generatedByteCode.apply(InternalRow(0.0)))
  println(generatedByteCode(InternalRow(1.0)))
  println(generatedByteCode(InternalRow(2.0)))
  println(Try(generatedByteCode(InternalRow(1))))
  println(Try(generatedByteCode(InternalRow("1.0"))))

}
