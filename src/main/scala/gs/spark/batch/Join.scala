package gs.spark.batch


import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gsantoro on 05/01/15.
 */
object Join {
  def main(args: Array[String]) {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    case class Register (d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float)
    case class Click (d: java.util.Date, uuid: String, landing_page: Int)

    val sparkConf = new SparkConf().setAppName("Join")
    val sc = new SparkContext(sparkConf)

    val reg = sc.textFile("src/main/resources/reg.tsv").map(_.split("\t")).map(
      r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat))
    )

    val clk = sc.textFile("src/main/resources/clk.tsv").map(_.split("\t")).map(
      c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt))
    )
    
    reg.join(clk).take(2).foreach(println)
//    val join = reg.join(clk)
//
//    println(join.toDebugString)
  }

}
