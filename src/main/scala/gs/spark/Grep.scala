package gs.spark

import gs.spark.util.{SparkJobs, DefaultConfigParser}
import gs.spark.util.gs.spark.util.Utils
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

object Grep {
  def grepRegex(regex: Regex)(input: RDD[String]): RDD[String] = {
    input
      .flatMap { s =>
        regex.findAllIn(s).matchData.map(m => m.group(1))
      }
      .filter(!_.isEmpty)
      .distinct()
  }

  def main(args: Array[String]) {
    val appName = "distributed grep"
    val regex = "externalLink=\"(.*?)\"".r

    DefaultConfigParser.parse(args, appName, (config) => {
      Utils.time(() => {
        SparkJobs.stringSparkJob(appName, config, grepRegex(regex))
      })
    })
  }
}
