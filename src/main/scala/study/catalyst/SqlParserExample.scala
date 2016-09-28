package study.catalyst

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import scala.util.Try

/**
  *
  */
object SqlParserExample extends App {

  val parsedPlan = CatalystSqlParser.parsePlan("Select * from people where age > 10")

  println(parsedPlan)

  // This one fails since there is no database, table and table structure available in the catalog
  println(Try(SimpleAnalyzer.execute(parsedPlan)))

}
