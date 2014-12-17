import org.apache.spark.SparkContext

/**
 * Created by gsantoro on 17/12/14.
 */
object Teenagers {
  case class Person(name: String, age: Int)

  def main(args: Array[String]) {
    val peopleFile = "src/main/resources/people.txt"
    val sc = new SparkContext("local", "Adolescent")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    val people = sc.textFile(peopleFile)
      .map(_.split(","))
      .map(p => Person(p(0), p(1).trim.toInt))

    people.registerTempTable("people")

    val teenagers = sqlContext.sql("SELECT name,age FROM people WHERE age >=20")

    teenagers.map(t => "Name: " + t(0) + " age: " + t(1)).foreach(println)
  }
}
