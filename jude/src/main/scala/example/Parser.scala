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
  type Review = (
    rating: Double,
    title: String, 
    text: String, 
    images: Array[String],
    asin: String,
    parent_asin: String, 
    user_id: String,
    timestamp: Int,
    verified_purchase: Boolean,
    helpful_vote: Int
  )

  // NOTE: details is a dictionary and might be hard to parse
  type Metadata = (
    main_category: String, 
    title: String, 
    average_rating: String,
    rating_number: Int,
    features: Array[String],
    description: Array[String],
    price: Double,
    images: Array[String],
    videos: Array[String],
    store: String, 
    categories: Array[String],
    details: Map[String, String],
    parent_asin: String,
    bought_together: String
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

  def arr(m: Map[String, Any], k: String): Array[String] =
    m.get(k).map {
      case xs: List[Any] => xs.map(_.toString).toArray
      case _ => Array[String]()
    }.getOrElse(Array[String]())

  def detailsMap(m: Map[String, Any], k: String): Map[String, String] =
    m.get(k).map {
      case d: Map[String, Any] =>
        d.map { case (key, value) => key -> value.toString }
      case _ => Map[String, String]()
    }.getOrElse(Map[String, String]())

  def bool(m: Map[String, Any], k: String): Boolean =
    m.get(k).map {
      case b: Boolean => b
      case s: String => s.toBoolean
      case _ => false
    }.getOrElse(false)

  // ===========================================================================

  // Input: A line of the json file metadata "{...}," or "{...}"
  // Output: A tuple (Metadata) "(rating, title, ...)"
  // Drops any tuples that weren't parsed correctly
  // MAKE SURE to filter for empty tuples in the result
  //  ex) val parsed = lines.map(parseReviews).filter(_ != null)
  def parseMetadata(line: String): Metadata = {
    JSON.parseFull(line.stripSuffix(",")) match {
      case Some(m: Map[String, Any]) => (
        str(m, "main_category"),
        str(m, "title"),
        str(m, "average_rating"),
        int(m, "rating_number"),
        arr(m, "features"),
        arr(m, "description"),
        double(m, "price"),
        arr(m, "images"),
        arr(m, "videos"),
        str(m, "store"),
        arr(m, "categories"),
        detailsMap(m, "details"),
        str(m, "parent_asin"),
        str(m, "bought_together")
      )
      case _ => null.asInstanceOf[Metadata]
    }
  }

  // Input: A line of the json file reviews "{...}," or "{...}"
  // Output: A tuple (Review) "(main_category, title, ...)"
  // Drops any tuples that weren't parsed correctly
  // MAKE SURE to filter for empty tuples in the result
  def parseReviews(line: String): Review = {
    JSON.parseFull(line.stripSuffix(",")) match {
      case Some(m: Map[String, Any]) => (
        double(m, "rating"),
        str(m, "title"),
        str(m, "text"),
        arr(m, "images"),
        str(m, "asin"),
        str(m, "parent_asin"),
        str(m, "user_id"),
        int(m, "timestamp") match {
          case 0 => int(m, "sort_timestamp")
          case t => t
        },
        bool(m, "verified_purchase"),
        int(m, "helpful_vote") match {
          case 0 => int(m, "helpful_votes")
          case h => h
        })
      case _ => null.asInstanceOf[Review]
    }
  }
}
