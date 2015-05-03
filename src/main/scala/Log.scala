import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gsantoro on 05/01/15.
 */
object Log {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log")
    val sc = new SparkContext(sparkConf)
    
    // base RDD
    val lines = sc.textFile("src/main/resources/log.txt")

    // transformed RDDs
    val errors = lines.filter(_.startsWith("ERROR"))
    val messages = errors.map(_.split("\t")).map(r => r(1))
    messages.cache()

    // actions
    val mysqlErrors = messages.filter(_.contains("mysql")).count()
    println(s"MySql errors: $mysqlErrors")
    
    val phpErrors =  messages.filter(_.contains("php")).count()
    println(s"Php errors: $phpErrors")
  }
}
