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
      "processed") // I don't know if this is going to the HDFS /usr/USER/output directory automatically...

  }

  def preprocessData(sc: SparkContext, reviewsPath: String, metadatPath: String, processedPath: String) = {
    /*******************/
    /* Inital Datasets */
    /*******************/
    // Reviews Dataset
    val reviewsRdd = sc.textFile(reviewsPath)
      .map(Parser.parseReviews)
      .filter(_ != null)

    // Metadata Dataset
    val metadataRdd = sc.textFile(metadatPath)
      .map(Parser.parseMetadata)
      .filter(_ != null)

    /*********************/
    /* JOIN the datasets */
    /*********************/
    // Join the reviews and the metadata on the parent_asin number
    val joined = reviewsRdd.cartesian(metadataRdd)
      .filter({
        // Review._6 is the parent_asin
        // Metadata._13 is the parent_asin
        case (review, metadata) => review._6 == metadata._13
        case _ => throw new IllegalArgumentException("Programmer's Fault...")
      })
  
    /*****************/
    /* Group by User */
    /*****************/
    // average_rating is in Metadata._3 (Double)
    // price is in Metadata._7 (Double)
    // parent_asin is in Metadata._13 (Double)
    // rating is in Review._1 (Double)
    // user_id is in Review._7 (String)

    // Only filter out the relevant information,
    // and convert it into a usable key-value pair
    // Key: user_id, Value: relevant information tuple
      .map({
        case (r, m) =>
          // (user_id, (rating, average_rating, price, parent_asin))
          (r._7, (r._1, m._3, m._13))
      })
  
    // For each user, associate an array of reviews a user has made
    .groupByKey()

    /****************/
    /* Save to File */
    /****************/
    .saveAsTextFile(processedPath)
  }
}
