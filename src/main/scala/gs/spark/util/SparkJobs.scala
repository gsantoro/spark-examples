package gs.spark.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gsantoro on 15/03/2016.
  */

object SparkJobs {
  def stringSparkJob(appName: String, config: DefaultConfig, process: RDD[String] => RDD[String]) = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)

    var input = sc.textFile(config.input)

    if (config.debug)
      input = input.sample(withReplacement = false, fraction = config.sample_fraction)

    input = input.repartition(config.partitions)

    val result = process(input)

    if (config.debug) {
      result.foreach(println)
    }
    else {
      result.saveAsTextFile(config.output)
    }
  }

  def tupleSparkJob(appName: String, config: DefaultConfig,
                    process: RDD[String] => RDD[(String, String)]) = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)

    var input = sc.textFile(config.input)

    if (config.debug)
      input = input.sample(withReplacement = false, fraction = config.sample_fraction)

    input = input.repartition(config.partitions)

    val result = process(input)

    if (config.debug) {
      result.foreach(println)
    }
    else {
      result.saveAsTextFile(config.output)
    }
  }
}
