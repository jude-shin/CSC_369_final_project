package example

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object App {

  def parseProcessedLine(line: String): ((String,String), Double) = {
    val pattern = """\(\(([^,]+),([^)]+)\),([0-9.]+)\)""".r
    line match {
      case pattern(user, product, rating) => ((user, product), rating.toDouble)
      case _ => throw new RuntimeException(s"Could not parse: $line")
    }
  }

  def main(args: Array[String]) {
    // Don't log a bunch of the junk
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark setup
    val conf = new SparkConf().setAppName("App")
    val sc = new SparkContext(conf)

    val processed =
      sc.textFile("processed/*").map(parseProcessedLine)

    // Currently, just tests the first 20 lines
    val testCases =
      processed.take(20)

    testCases.foreach {
      case ((u, p), actual) =>

        val pred =
          Metric.predictRating(
            processed,
            u,
            p,
            Pearson.pearsonCorrelation,
            10 // Top N neighbors used for comparison
          )

        // Just prints for now ??
        println(s"$u,$p => actual=$actual pred=$pred")
    }

    sc.stop()

  }
}
