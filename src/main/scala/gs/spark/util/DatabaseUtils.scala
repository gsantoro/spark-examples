package gs.spark.util

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

/**
  * Created by gsantoro on 16/03/2016.
  */
object DatabasesUtils {
  implicit class ESUtil[T](val rdd: RDD[(String, T)]) {
    //NOTE: USE client.transport.ignore_cluster_name IN THE ES CONFIG

    def saveToES(conf: Map[String, String]): Unit = {
      rdd.foreachPartition { partition =>
        if (partition.nonEmpty) {
          val client = TransportClient.builder().build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(conf("host")), conf("port").toInt))

          //NOTE: this is for version ES 1.6
//          val client = new TransportClient()
//            .addTransportAddress(new InetSocketTransportAddress(conf("host"), conf("port").toInt))

          val bulkRequest = client.prepareBulk()

          partition.foreach { case (uuid, entry) =>
            val document = client.prepareIndex(conf("index"), conf("type")).setId(uuid).setSource(entry.toString)
            bulkRequest.add(document)
          }

          bulkRequest.execute().actionGet()
          client.close()
        }
      }
    }
  }
}