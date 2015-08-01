import org.apache.spark.{SparkConf, SparkContext}

object FirstNWords {
  def main(args: Array[String]): Unit = {
    val logFile = "src/main/resources/README.md"
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    sc.textFile(logFile, 2)
      .flatMap(_.split("\\s+"))
      .filter(_.trim.length > 0)
      .map((_, 1))
      .reduceByKey(_ + _)
      .top(10)(tupleOrdering).foreach(println)
  }
}

object tupleOrdering extends Ordering[(String,Int)] {
  override def compare(x: (String,Int), y: (String,Int)): Int = x._2.compare(y._2)
}