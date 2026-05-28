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

    // Only have to do once
    preprocessData(sc, 
      "input/final/reviews/", 
      "input/final/metadata/",
      "processed")
  }

  def preprocessData(sc: SparkContext, reviewsPath: String, metadatPath: String, processedPath: String) = {
    /*******************/
    /* Inital Datasets */
    /*******************/
    // Reviews Dataset
    val reviewsRdd = sc.textFile(reviewsPath)
      .map(Parser.parseReviews)
      .filter(_ != null)
      .map(r => (r._6, r))  // Review._6 is the parent_asin

    // Metadata Dataset
    val metadataRdd = sc.textFile(metadatPath)
      .map(Parser.parseMetadata)
      .filter(_ != null)
      .map(m => (m._13, m)) // Metadata._13 is the parent_asin

    /*********************/
    /* JOIN the datasets */
    /*********************/
    // Join the reviews and the metadata on the parent_asin number
    val joined = reviewsRdd.join(metadataRdd)

    joined
      .map ({
        case (parent_asin, (r, m)) =>
          val rating = r._1
          val user_id = r._3 // assuming Review._3 is user_id
          ((user_id, parent_asin), rating)
      })

    /****************/
    /* Save to File */
    /****************/
    .saveAsTextFile(processedPath)
  }
}
