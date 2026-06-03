package example

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import scala.util.parsing.json.JSON

object Cosine {

  // data is a ((user_id, parent_asin), rating) tuple
  type ProcessedRDD = RDD[((String, String), Double)]

  // a user is represented as a sparse map from product asin to their rating
  type UserVector = Map[String, Double]

  
  def buildUserVectors(data: ProcessedRDD): Map[String, UserVector] = {
    data
      .map { case ((userId, asin), rating) => (userId, (asin, rating)) }
      .groupByKey()
      .mapValues(pairs => pairs.toMap)
      .collectAsMap()
      .toMap
  }

  def cosine(a: UserVector, b: UserVector): Double = {
    // dot product formula per https://en.wikipedia.org/wiki/Cosine_similarity
    val dot = a.foldLeft(0.0) {
      case (acc, (asin, ratingA)) => acc + ratingA * b.getOrElse(asin, 0.0)
    }

    val normA = math.sqrt(a.values.map(r => r * r).sum)
    val normB = math.sqrt(b.values.map(r => r * r).sum)
    val denom = normA * normB

    if (denom == 0.0) 0.0 else dot / denom
  }

  // returns the k closest users to queryUserId, sorted by similarity descending
  def kMostSimilar(
    queryUserId: String,
    userVectors: Map[String, UserVector],
    k: Int
  ): List[(String, Double)] = {
    val queryVec = userVectors.getOrElse(queryUserId, Map.empty)

    userVectors
      .filterKeys(_ != queryUserId)
      .map { case (userId, vec) => (userId, cosine(queryVec, vec)) }
      .toList
      .sortBy(-_._2)
      .take(k)
  }

 
}
