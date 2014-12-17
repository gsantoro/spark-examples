import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object FirstNWords {
  def main(args: Array[String]): Unit = {
    val logFile = "src/main/resources/README.md"
    val sc = new SparkContext("local", "First N Words")

    val wc = sc.textFile(logFile, 2)
      .flatMap(_.split("\\s+"))
      .filter(_.trim.length > 0)
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(v => (v._2, v._1))
      .top(10)

    wc.foreach(println)
  }
}