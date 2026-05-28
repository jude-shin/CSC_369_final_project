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
  
    /*****************/
    /* Group by User */
    /*****************/
    // parent_asin is in Metadata._13 (Double)
    // rating is in Review._1 (Double)

    // Only filter out the relevant information,
    // and convert it into a usable key-value pair
    // Key: user_id, Value: relevant information tuple

    // (user_id, [(rating, parent_asin)])
      .map({ 
        case (parent_asin, (r, m)) => (r._7, (r._1, m._13)) 
      })
      .flatMapValues(t => t)
      .map({
        case (user_id, (rating, parent_asin)) => ((userId, parent_asin), rating)
      })

    // ((user_id, parent_asin), rating)
  
    // For each user, associate an array of reviews a user has made
    .groupByKey()

    /****************/
    /* Save to File */
    /****************/
    .saveAsTextFile(processedPath)
  }
}
