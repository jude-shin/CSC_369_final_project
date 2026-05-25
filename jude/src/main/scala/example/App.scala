package example

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object App {
  def main(args: Array[String]) {
    // Don't log a bunch of the junk
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark setup
    val conf = new SparkConf().setAppName("App")
    val sc = new SparkContext(conf)

    // Reviews Dataset
    val reviewsRdd = sc.textFile("input/final/reviews/")
      .map(Parser.parseReviews)
      .filter(_ != null)

    // Metadata Dataset
    val metadataRdd = sc.textFile("input/final/metadata/")
      .map(Parser.parseMetadata)
      .filter(_ != null)

    // Join the reviews and the metadata on the parent_asin number
    var rdd = reviewsRdd.cartesian(metadataRdd)
      .filter({
        // Review._6 is the parent_asin
        // Metadata._13 is the parent_asin
        case (review, metadata) => review._6 == metadata._13
        case _ => throw new IllegalArgumentException("Programmer's Fault...")
      })
      .take(3).foreach(println)
    
  }
}
