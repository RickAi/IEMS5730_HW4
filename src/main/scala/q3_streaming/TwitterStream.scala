package q3_streaming

import java.io.{File, FileOutputStream}

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.TwitterFactory
import twitter4j.auth.AccessToken

// part of the code were referenced from:
// https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/scala/org/apache/spark/examples/streaming/twitter/TwitterPopularTags.scala
object TwitterStream {
  def main(args: Array[String]) {

    val consumerKey = "I3ocH634s4KMuySLGP5tHalTv"
    val consumerSecret = "Sqhv5aiPyMAdkzHOYHfrjE8bPbzvkbLeslXP3m2wlGHjALAlcx"
    val accessToken = "804585635767930880-Zb4lDk1iFv51PzeECsscu0PZ2y2xNue"
    val accessTokenSecret = "Cthb9m5z8bEIozEdgJv6ww9XNMswDrmjed0OZ3LOBQAvP"

    val sparkConf = new SparkConf().setAppName("TwitterStream")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val twitter = new TwitterFactory().getInstance()
    twitter.setOAuthConsumer(consumerKey, consumerSecret)
    twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, Option(twitter.getAuthorization()), Array[String]("Trump"))

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topCounts = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _,
      windowDuration = Minutes(10), slideDuration = Minutes(2))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(ascending = false))

    val fos = new FileOutputStream(new File("/home/yongbiaoai/twitter_output.txt"))
    // Print popular hashtags
    topCounts.foreachRDD(rdd => {
      val topList = rdd.take(10)
      Console.withOut(fos) {
        println("\nTop-10 popular topics in last 10 minutes (%s total):".format(rdd.count()))
      }
      topList.foreach { case (count, tag) =>
        Console.withOut(fos) {
          println("%s (%s tweets)".format(tag, count))
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}