import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Created by gsantoro on 04/05/15.
 */
object UsersGraph {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(
      Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))
      )
    )

    val relationships: RDD[Edge[String]] = sc.parallelize(
      Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi")
      )
    )

    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(users, relationships, defaultUser)

    val postdocs = graph.vertices.filter {
      case (id, (name, pos)) =>
        pos == "postdoc"
    }.count()
    println(s"Postdocs: $postdocs")

    val edgesGT = graph.edges.filter(e => e.srcId > e.dstId).count
    println(s"Src greater than dst: $edgesGT")

    graph.edges.filter(e => e.srcId == 5L).map(e => e.attr).foreach { edgeAttr =>
      println(s"Connection from 5L: $edgeAttr")
    }

    graph.edges.filter(_.dstId == 7L).map(_.attr).foreach{ edgeAttr =>
      println(s"Connection to 7L: $edgeAttr")
    }

    graph.triplets.map( triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    graph.inDegrees.foreach(inDegree => println(s"InDegree: $inDegree"))
    graph.outDegrees.foreach(outDegree => println(s"OutDegree: $outDegree"))
  }
}
