package example

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import scala.util.parsing.json.JSON

object Parser {
  // Type alias tuples with named fields
  // (_, _, _, _, _, _, _, _, _, _, _, _, _, _)
  // (main_category, title, average_rating, rating_number, features, description, price, images, videos, store, categories, details, parent_asin, bought_together)
  type Metadata = (
    String,                    // main_category
    String,                    // title
    Double,                    // average_rating
    Int,                       // rating_number
    Array[String],             // features
    Array[String],             // description
    Double,                    // price
    Array[Map[String, String]],// images
    Array[String],             // videos
    String,                    // store
    Array[String],             // categories
    Map[String, String],       // details
    String,                    // parent_asin
    String                     // bought_together
  )

  // NOTE: details is a dictionary and might be hard to parse
  // (_, _, _, _, _, _, _, _, _, _)
  // (rating, title, text, images, asin, parent_asin, user_id, timestamp, verified_purchase, helpful_vote)
  type Review = (
    Double,                     // rating
    String,                     // title
    String,                     // text
    Array[Map[String, String]], // images
    String,                     // asin
    String,                     // parent_asin
    String,                     // user_id
    Long,                       // timestamp
    Boolean,                    // verified_purchase
    Int                         // helpful_vote
  )

  // ===========================================================================

  def str(m: Map[String, Any], k: String): String =
    m.get(k).map {
      case null => ""
      case x => x.toString
    }.getOrElse("")

  def double(m: Map[String, Any], k: String): Double =
    m.get(k).map {
      case d: Double => d
      case i: Int => i.toDouble
      case s: String => s.toDouble
      case _ => 0.0
    }.getOrElse(0.0)

  def int(m: Map[String, Any], k: String): Int =
    m.get(k).map {
      case d: Double =>
        if (d > Int.MaxValue) (d / 1000).toInt else d.toInt
      case i: Int => i
      case s: String => s.toInt
      case _ => 0
    }.getOrElse(0)

  def bool(m: Map[String, Any], k: String): Boolean =
    m.get(k).map {
      case b: Boolean => b
      case s: String => s.toBoolean
      case _ => false
    }.getOrElse(false)

  def long(m: Map[String, Any], k: String): Long =
    m.get(k).map {
      case d: Double => d.toLong
      case i: Int => i.toLong
      case l: Long => l
      case s: String => s.toLong
      case _ => 0L
    }.getOrElse(0L)

  def arr(m: Map[String, Any], k: String): Array[String] =
    m.get(k).map {
      case xs: List[Any] => xs.map(_.toString).toArray
      case _ => Array[String]()
    }.getOrElse(Array[String]())

  def arrMap(m: Map[String, Any], k: String): Array[Map[String, String]] =
    m.get(k).map {
      case xs: List[Any] =>
        xs.collect {
          case d: Map[String, Any] =>
            d.map {
              case (key, null) => key -> ""
              case (key, value) => key -> value.toString
            }
        }.toArray
              case _ => Array[Map[String, String]]()
    }.getOrElse(Array[Map[String, String]]())

  def detailsMap(m: Map[String, Any], k: String): Map[String, String] =
    m.get(k).map {
      case d: Map[String, Any] =>
        d.map { case (key, value) => key -> value.toString }
      case _ => Map[String, String]()
    }.getOrElse(Map[String, String]())

  // ===========================================================================

  // Input: A line of the json file metadata "{...}," or "{...}"
  // Output: A tuple (Metadata) "(rating, title, ...)"
  // Drops any tuples that weren't parsed correctly
  // MAKE SURE to filter for empty tuples in the result
  //  ex) val parsed = lines.map(parseReviews).filter(_ != null)
  def parseMetadata(line: String): Metadata = {
    try {
      JSON.parseFull(line.stripSuffix(",")) match {
        case Some(m: Map[String, Any]) => (
          str(m, "main_category"),
          str(m, "title"),
          double(m, "average_rating"),
          int(m, "rating_number"),
          arr(m, "features"),
          arr(m, "description"),
          double(m, "price"),
          arrMap(m, "images"),
          arr(m, "videos"),
          str(m, "store"),
          arr(m, "categories"),
          detailsMap(m, "details"),
          str(m, "parent_asin"),
          str(m, "bought_together")
        )
        case _ => null.asInstanceOf[Metadata]
      }
    } catch {
      case _: Throwable => null.asInstanceOf[Metadata]
    }
  }

  // Input: A line of the json file reviews "{...}," or "{...}"
  // Output: A tuple (Review) "(main_category, title, ...)"
  // Drops any tuples that weren't parsed correctly
  // MAKE SURE to filter for empty tuples in the result
  def parseReviews(line: String): Review = {
    try {
      JSON.parseFull(line.stripSuffix(",")) match {
        case Some(m: Map[String, Any]) => (
          double(m, "rating"),
          str(m, "title"),
          str(m, "text"),
          arrMap(m, "images"),
          str(m, "asin"),
          str(m, "parent_asin"),
          str(m, "user_id"),
          long(m, "timestamp") match {
            case 0L => long(m, "sort_timestamp")
            case t => t
          },
          bool(m, "verified_purchase"),
          int(m, "helpful_vote") match {
            case 0 => int(m, "helpful_votes")
            case h => h
          }
          )
        case _ => null.asInstanceOf[Review]
      }
    } catch {
      case _: Throwable => null.asInstanceOf[Review]
    }
  }

  // =========================================================================== 

  def preprocessData(sc: SparkContext, reviewsPath: String, metadatPath: String, processedPath: String) = {
    /*******************/
    /* Inital Datasets */
    /*******************/
    // Reviews Dataset
    val reviewsRdd = sc.textFile(reviewsPath)
      .map(parseReviews)
      .filter(_ != null)
      .map(r => (r._6, r))  // Review._6 is the parent_asin

    // Metadata Dataset
    val metadataRdd = sc.textFile(metadatPath)
      .map(parseMetadata)
      .filter(_ != null)
      .map(m => (m._13, m)) // Metadata._13 is the parent_asin

    /*********************/
    /* JOIN the datasets */
    /*********************/
    // Join the reviews and the metadata on the parent_asin number
    val joined = reviewsRdd.join(metadataRdd)

    // rating = r._1 
    // user_id = r._7
    joined
      .map ({
        case (parent_asin, (r, m)) => ((r._7, parent_asin), r._1)
      })

    /****************/
    /* Save to File */
    /****************/
    .saveAsTextFile(processedPath)
  }

}
