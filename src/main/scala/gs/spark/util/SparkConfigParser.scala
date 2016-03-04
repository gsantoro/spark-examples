package gs.spark.util

import scopt.OptionParser

object SparkConfigParser {
  def parse(args: Array[String], app: String = "", process: (SparkConfig) => Unit) = {
    val parser = new OptionParser[SparkConfig](app) {
      head("sampler", "0.0.1")
      opt[String]('i', "input") required() valueName ("<input>") action { (x, c) =>
        c.copy(input = x)
      } text ("(required) where to read from")
      opt[String]('o', "output") valueName ("<output>") action { (x, c) =>
        c.copy(output = x)
      } text ("(optional) where to store the result of the sampling")
      opt[Int]('p', "partitions") valueName ("<partitions>") action { (x, c) =>
        c.copy(partitions = x)
      } text ("(optional, default 2) number of partitions to split the output into")
      opt[Double]('f', "sample_fraction") valueName ("<fraction>") action { (x, c) =>
        c.copy(sample_fraction = x)
      } text ("(optional, default 0.01) fraction of entries to store")
      opt[Boolean]('d', "debug") valueName ("<debug>") action { (x, c) =>
        c.copy(debug = x)
      } text ("(optional, default true) debug = true print to stdout instead of file")
      help("help") text ("prints this usage text")
    }

    parser.parse(args, SparkConfig()) match {
      case Some(config) =>
        print(s"Configuration: $config")

        process(config)
      case None =>
    }
  }
}

case class SparkConfig(input: String = null,
                       output: String = null,
                       partitions: Int = 2,
                       sample_fraction: Double = 0.01,
                       debug: Boolean = true)