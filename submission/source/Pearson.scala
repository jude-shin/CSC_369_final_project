package example
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import scala.util.parsing.json.JSON

/*
Pearsons Correlation Coefficient Algorithm Returns a value between -1 to 1 indicating the correlation 
where 1 is perfect positive correlation (2 users rate items identically), 0 is no correlation, 
and -1 is perfect negative correlation (2 users rate items oppositely)

pearsonCorrelation(
        user1Ratings: Iterable[(String, Double)], 
        user2Ratings: Iterable[(String, Double)] 
    ): Double

userRatings is all ratings for given user as a (parent_asin, rating) pair
*/
object Pearson {

    /* 
    1. find co-rated items (intersection by parent_asin)
    2. if no co-rated items → return 0.0
    3. compute mean rating for each user (over co-rated items only)
    4. for each co-rated item:
        numerator   += (r1 - mean1) * (r2 - mean2)
        denominator += (r1 - mean1)^2  and  (r2 - mean2)^2
    5. denominator = sqrt(denom1 * denom2)
    6. if denominator == 0 → return 0.0
    7. return numerator / denominator 
    */

    def pearsonCorrelation(
        user1Ratings: Iterable[(String, Double)], 
        user2Ratings: Iterable[(String, Double)] 
    ): Double = {
        val user1Map = user1Ratings.toMap
        val user2Map = user2Ratings.toMap

        val coRatedItems = user1Map.keySet.intersect(user2Map.keySet)

        if (coRatedItems.isEmpty) return 0.0

        val mean1 = coRatedItems.map(item => user1Map(item)).sum / coRatedItems.size
        val mean2 = coRatedItems.map(item => user2Map(item)).sum / coRatedItems.size

        var numerator = 0.0
        var denom1 = 0.0
        var denom2 = 0.0

        for (item <- coRatedItems) {
            val r1 = user1Map(item)
            val r2 = user2Map(item)

            numerator += (r1 - mean1) * (r2 - mean2)
            denom1 += math.pow(r1 - mean1, 2)
            denom2 += math.pow(r2 - mean2, 2)
        }

        val denominator = math.sqrt(denom1 * denom2)

        if (denominator == 0) return 0.0

        numerator / denominator
    }
}
