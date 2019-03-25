package q1_basic

import org.apache.spark.sql.SparkSession

class NaivePageRank {

  val spark = SparkSession.builder.
    master("spark://instance-1:7077").
    appName("NaivePageRank").
    getOrCreate()
  val sc = spark.sparkContext

  var links = sc.
}
