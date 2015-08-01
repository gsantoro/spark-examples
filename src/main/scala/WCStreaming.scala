import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
/**
 * Created by gsantoro on 17/12/14.
 */
object WCStreaming {

  def main(args: Array[String]) {
    /*
     * Run first "nc -lk 1337" and then this class without parameter
     */
    var serverIP = "localhost"
    var serverPort = 1337

    if (args.length >2) {
      serverIP = args(0)
      serverPort = args(1).trim.toInt
    }

    val sparkConf = new SparkConf().setAppName("WCStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.socketTextStream(serverIP, serverPort)

    val wc = lines
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_ + _)

    wc.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
