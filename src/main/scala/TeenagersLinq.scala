import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gsantoro on 17/12/14.
 */
object TeenagersLinq {
  case class Person(name: String, age: Int)

  def main(args: Array[String]) {
//    val peopleFile = "src/main/resources/people.txt"
//    val sparkConf = new SparkConf().setAppName("Adolescent")
//    val sc = new SparkContext(sparkConf)
//
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext._
//
//    val people = sc.textFile(peopleFile)
//      .map(_.split(","))
//      .map(p => Person(p(0), p(1).trim.toInt))
//
//    people.registerTempTable("people")
//
//    val teenagers = people.where('age >= 13).where('age <= 19).select('name, 'age)
//
//    teenagers.map(t => "Name: " + t(0) + " age: " + t(1)).foreach(println)
  }
}
