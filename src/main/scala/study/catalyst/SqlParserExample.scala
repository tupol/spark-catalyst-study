package study.catalyst

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  *
  */
object SqlParserExample extends App {

  val parsedPlan = CatalystSqlParser.parsePlan("Select * from people where age > 10")

  println(parsedPlan)

  // This one fails since there is no database, table and table structure available in the catalog
  val analyzedPlan: LogicalPlan = SimpleAnalyzer.execute(parsedPlan)

}
