package tasks

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by gsantoro on 25/11/2015.
  */
object Main {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Tasks.Main")
    val sc = new SparkContext(sparkConf)

    var index = 0
    for (index <- 1 to 10) {
      val rdd = sc.parallelize(1 to 100, 100)

      val result = rdd.map {
        case i =>
          Thread.sleep(100)
          (index, i + 10, Thread.currentThread().getName)
      }

      result.foreach(println)
    }
  }
}
