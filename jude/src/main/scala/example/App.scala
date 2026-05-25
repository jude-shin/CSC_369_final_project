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

    // Metadata Dataset
    val metadataRdd = sc.textFile("input/final/metadata/")
      .map(l => Parser.parseMetadata(l))
      .take(3).foreach(println)

    // Reviews Dataset

    // Join the reviews and the metadata on the parent_asin number
    // var rdd = reviewsRdd.cartesian(metadataRdd)
    //   .filter({
    //     // ((ename, did), (did2, dname))
    //     // empl._2 and dept._1 is the did
    //     case (empl, dept) => empl._2 == dept._1
    //     case _ => throw new IllegalArgumentException("You are the problem... You should never be here!")
    //   })

    
  }
}
