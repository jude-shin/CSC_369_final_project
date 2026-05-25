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
      .map({
        case (rating, title, text, images, asin, parent_asin, user_id, timestamp, verified_purchase, helpful_vote) => (parent_asin, (rating, title, text, images, asin, parent_asin, user_id, timestamp, verified_purchase, helpful_vote))
      })

    // Metadata Dataset
    val metadataRdd = sc.textFile("input/final/metadata/")
      .map(Parser.parseMetadata)
      .filter(_ != null)
      .map({
        case (main_category, title, average_rating, rating_number, features, description, price, images, videos, store, categories, details, parent_asin, bought_together) =>
          (parent_asin, (main_category, title, average_rating, rating_number, features, description, price, images, videos, store, categories, details, parent_asin, bought_together))
      })

    // Join the reviews and the metadata on the parent_asin number
    var rdd = reviewsRdd.join(metadataRdd)
      .take(3).foreach(println)
    
  }
}
