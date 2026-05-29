import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import scala.util.parsing.json.JSON

/*
def findMostSimilarUsers(
    processed: RDD[((String, String), Double)],  // ((user_id, parent_asin), rating)
    targetUser: String,
    similarityFn: (Iterable[(String, Double)], Iterable[(String, Double)]) => Double, (pearsons or cosine)
    topN: Int = 10
): Array[((String, String), Double)]


*/
object MostSimilar {
    /* 
    1. rearrange RDD fromat
    ((user_id, parent_asin), rating) to (user_id, (parent_asin, rating))
    2. group by (user_id, Iterable[(parent_asin, rating)])
    3. collect() just the target user's profile to driver
    4. filter out target user from RDD
    5. .map over remaining users:
        score = pearsonCorrelation(targetProfile, theirProfile)
        → ((targetUser, otherUser), score)
    6. .top(topN) ordered by score → return

     */

    def findMostSimilarUsers(
        processed: RDD[((String, String), Double)],  // ((user_id, parent_asin), rating)
        targetUser: String,
        similarityFn: (Iterable[(String, Double)], Iterable[(String, Double)]) => Double, // pearsons or cosine
        topN: Int = 10
    ): Array[((String, String), Double)] = {

        val userRatings = processed.map {
            case ((userId, parentAsin), rating) => (userId, (parentAsin, rating))
        }.groupByKey()

        //just ratings of target user
        val targetProfile = userRatings.filter {
            case (userId, _) => userId == targetUser
        }.collect().head._2

        //everyone but target user
        userRatings.filter {
            case (userId, _) => userId != targetUser
        }.map {
            case (otherUser, ratings) =>
                // compute similarity score between ratings from target user and other user's ratings
                val score = similarityFn(targetProfile, ratings)
                ((targetUser, otherUser), score)
        }.top(topN)(Ordering.by(_._2)) //order by score
    }

}