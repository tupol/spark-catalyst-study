package study.catalyst.pfa

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DoubleType

/**
  *
  */
object PFA extends App {
  
  // Define a simple attribute for out "variable" x
  val x = AttributeReference("x", DoubleType)()

  println(s"The x attribute is: $x")

  //Define a simple add expression that would look something like `x + ( 1 + 2 )`
  val addExpressionRaw = Add( x, Add( Literal(1.0), Literal(2.0) ) )


  def exprs2PFA(exps: Seq[Expression]) = {
    exps.map(expr2PFA(_)).mkString(",\n")
  }

  def expr2PFA(expr : Expression): String = expr match {
    case e : Literal => s"""${e.value}"""
    case e : Attribute => s""""input.${e.name}""""
    case e : Alias => s"""${expr2PFA(e.child)}"""
    case e : Cast => println(s"[[[$expr]]"); s"""{"???cast???: ${expr2PFA(e.child)} to ${e.dataType} }"""
    case _ : Add => s"""{"+": [${exprs2PFA(expr.children)}] }"""
    case _ : Subtract => s"""{"-": [${exprs2PFA(expr.children)}] }"""
    case _ : Multiply => s"""{"+": [${exprs2PFA(expr.children)}] }"""
    case _ : Divide => s"""{"/": [${exprs2PFA(expr.children)}] }"""
    case e : Hour => s"""{"time.hourOfDay": [${expr2PFA(e.child)}] }"""
    case e : Month => s"""{"time.monthOfYear": [${expr2PFA(e.child)}] }"""
    case e : DayOfMonth => s"""{"time.dayOfMonth": [${expr2PFA(e.child)}] }"""
    case e : Lower => s"""{"s.lower": [${expr2PFA(e.child)}] }"""
    case e : Upper => s"""{"s.upper": [${expr2PFA(e.child)}] }"""
    case e : StringTrim => s"""{"s.strip": [${expr2PFA(e.child)}] }"""
    case e : Or => s"""{"||": [${expr2PFA(e.left)}, ${expr2PFA(e.right)}] }"""
    case e : And => s"""{"&&": [${expr2PFA(e.left)}, ${expr2PFA(e.right)}] }"""
    case e : Not => s"""{"!": [${expr2PFA(e.child)}] }"""
    case e : EqualTo => s"""{"==": [${expr2PFA(e.left)}, ${expr2PFA(e.right)}] }"""
    case e : GreaterThan => s"""{">": [${expr2PFA(e.left)}, ${expr2PFA(e.right)}] }"""
    case e : GreaterThanOrEqual => s"""{">=": [${expr2PFA(e.left)}, ${expr2PFA(e.right)}] }"""
    case e : LessThan => s"""{"<": [${expr2PFA(e.left)}, ${expr2PFA(e.right)}] }"""
    case e : LessThanOrEqual => s"""{"<=": [${expr2PFA(e.left)}, ${expr2PFA(e.right)}] }"""
    case e : IsNull => s"""{"ifnotnull": {result: [${expr2PFA(e.child)}]}, "then": false, "else": true}"""
    case e : IsNotNull => s"""{"ifnotnull": {result: [${expr2PFA(e.child)}]}, "then": true, "else": false}"""
    case e : CaseWhenCodegen => s"""{"cond": [${ifCond(e.branches)}] ${e.elseValue match { case Some(eb) => s""", "else": [${expr2PFA(eb)}] """ }}] """
    case _ => println(s"[[[$expr]]"); expr.children.foreach(println); s"""**${expr.getClass.getSimpleName}**"""
  }

  def ifCond(ex: (Expression, Expression)): String = s"""{"if": [${expr2PFA(ex._1)}], "then": [${expr2PFA(ex._2)}] }"""
  def ifCond(exs: Seq[(Expression, Expression)]): String = exs.map(ifCond).mkString(", ")

  println(expr2PFA(addExpressionRaw))

}
