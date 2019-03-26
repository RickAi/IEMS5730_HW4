package q1_basic

import org.apache.spark.sql.SparkSession

object NaivePageRank {

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    val appName = if (args.length >= 1) args(0).toString else "NaivePageRank"
    val master = if (args.length >= 2) args(1).toString else "local"
    val filePath = if (args.length >= 3) args(2).toString else "./web-Google.txt_sample"

    println(s"AppName=$appName")
    println(s"Master=$master")
    println(s"FilePath=$filePath")

    val spark = SparkSession.builder.
      master(master).
      appName(appName).
      getOrCreate()
    val sc = spark.sparkContext

    var links = sc.textFile(filePath)
      .map(line => line.split("\t"))
      .map(x => (x(0).trim, x(1).trim))
      .distinct().groupByKey()

    var ranks = links.mapValues(v => 1.0)
    for (i <- 1 until 30) {
      val contribs = links.join(ranks).flatMap {
        case (nodeId, (links, rank)) =>
          links.map(dest => (dest, rank / links.size))
      }

      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    ranks.sortBy(_._2, ascending = false).take(100).foreach(tup => println(s"${tup._1}\t${tup._2}"))

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(s"Total time cost=${durationSeconds}s")
  }

}
