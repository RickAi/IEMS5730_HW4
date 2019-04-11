package q5_ml

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import scala.collection.breakOut

object MovieALS {

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    val appName = if (args.length >= 1) args(0).toString else "MovieALS"
    val master = if (args.length >= 2) args(1).toString else "local"
    val filePath = if (args.length >= 3) args(2).toString else "./ratings.csv"

    println(s"AppName=$appName")
    println(s"Master=$master")
    println(s"FilePath=$filePath")

    val spark = SparkSession.builder.
      master(master).
      appName(appName).
      getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    val ratings = sc.textFile(filePath).map { line =>
      // line format:
      // userId,movieId,rating,timestamp
      val fields = line.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.cache()

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val allMovies = ratings.map(_.product).distinct()
    val numMovies = allMovies.count()
    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    val model = ALS.train(ratings, 50, 10, 0.01)

    val userIds = Array(1, 1001, 10001)
    for (userId <- userIds) {
      val userMovies = allMovies.map(movieId => (userId, movieId))
      println(s"\nTop 10 favorite movies of user ${userId}")
      model.predict(userMovies)
        .sortBy(_.rating, ascending = false)
        .take(10)
        .foreach(rating => {
          println(s"${rating.user}\t${rating.product}\t${rating.rating}")
        })
      println("\n")
    }

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(s"Total time cost=${durationSeconds}s")

  }

}
