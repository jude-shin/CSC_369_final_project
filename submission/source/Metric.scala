package example

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import scala.util.parsing.json.JSON

object Metric {

  /*
   * Predict the rating targetUser would give targetProduct.
   * Returns a double
   *
   * Functions used:
   * processed:
   *   ((userId, productId), rating)
   *
   * similarityFn:
   *   Pearson, cosine, etc.
   */

  def predictRating(
    processed: RDD[((String, String), Double)],
    targetUser: String,
    targetProduct: String,
    similarityFn: (
      Iterable[(String, Double)],
        Iterable[(String, Double)]
      ) => Double, topN: Int): Double = {

    // Remove test entry if present
    val trainingData =
      processed.filter {
        case ((u, p), _) => !(u == targetUser && p == targetProduct)
      }

    // Build user profiles:
    // user -> Iterable[(product, rating)]
    val userRatings =
      trainingData
        .map {
          case ((userId, productId), rating) =>
            (userId, (productId, rating))
        }
        .groupByKey()
        .cache()

    // Target user's ratings
    val targetLookup =
      userRatings.lookup(targetUser)

    if (targetLookup.isEmpty)
      return 0.0

    val targetProfile = targetLookup.head

    // Compute similarity to all other users
    val similarities =
      userRatings
        .filter {
          case (userId, _) =>
            userId != targetUser
        }
        .map {
          case (otherUser, ratings) =>
            val similarity =
              similarityFn(
                targetProfile,
                ratings
              )
            (otherUser, similarity)
        }

    // Users who rated the target product
    val productRatings =
      trainingData
        .filter {
          case ((_, productId), _) =>
            productId == targetProduct
        }
        .map {
          case ((userId, _), rating) =>
            (userId, rating)
        }

    // (user, (similarity, rating))
    val neighbors =
      similarities.join(productRatings)

    // Keep top-N positively correlated users
    val nearest =
      neighbors
        .filter {
          case (_, (similarity, _)) =>
            similarity > 0.0
        }
        .takeOrdered(topN)(
          Ordering[Double]
            .reverse
            .on(_._2._1)
        )

    // No comparison -> make it neutral??
    if (nearest.isEmpty)
      return 0.0

    val numerator =
      nearest.map {
        case (_, (similarity, rating)) =>
          similarity * rating
      }.sum

    val denominator =
      nearest.map {
        case (_, (similarity, _)) =>
          math.abs(similarity)
      }.sum

    // Denominator == 0 -> no similars
    if (denominator == 0.0)
      0.0
    else
      numerator / denominator
  }
}