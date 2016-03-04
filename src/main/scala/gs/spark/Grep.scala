package gs.spark

import gs.spark.util.SparkConfigParser
import gs.spark.util.gs.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

object Grep {
  def main(args: Array[String]) {
    val appName = "distributed grep"
    val regex = "externalLink=\"(.*?)\"".r

    SparkConfigParser.parse(args, appName, (config) => {
      Utils.time(() => {
        val sparkConf = new SparkConf().setAppName(appName)
        val sc = new SparkContext(sparkConf)

        val result = sc.textFile(config.input)
          .repartition(config.partitions)
          .map { s =>
            regex.findAllIn(s).matchData.map(m => m.group(1)).mkString(" --- ")
          }
          .filter(!_.isEmpty)

        if (config.debug) {
          result.foreach(println)
        }
        else {
          result.saveAsTextFile(config.output)
        }
      })
    })
  }
}
