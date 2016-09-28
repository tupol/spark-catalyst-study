package study.catalyst

import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
  * A hack container for the minimal set of information to generate the projection and also describe the inputs and the outputs.
  *
  * Normally we would like a LogicalPlan instead of this.
  */
case class CodeGenParameters(code: CodeAndComment, references: Array[Any], metadata: Metadata)

case class Metadata(inputs: Seq[Attribute], outputs: Seq[Attribute])
