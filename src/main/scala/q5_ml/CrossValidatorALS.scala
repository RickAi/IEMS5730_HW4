package q5_ml

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession

object CrossValidatorALS {

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
    val trainingDF = spark.createDataFrame(ratings)
      .toDF("user", "movie", "rating")

    val als = new ALS().setItemCol("movie")

    val paramGrid = new ParamGridBuilder()
      .addGrid(als.regParam, Array(50, 10, 0.01))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol("rating").setMetricName("mse")

    val cv = new CrossValidator()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)
      .setParallelism(2)

    val cvModel = cv.fit(trainingDF)

    val userMovies = ratings.map(rating => {
      (rating.user, rating.product)
    })
    val testingDF = spark.createDataFrame(userMovies)
      .toDF("user", "movie")

    cvModel.transform(testingDF)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(s"Total time cost=${durationSeconds}s")

  }

}
