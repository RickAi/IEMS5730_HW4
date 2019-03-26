package q2_sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CrimeIncidents {

  def main(args: Array[String]): Unit = {

    val startTimeMillis = System.currentTimeMillis()

    val appName = if (args.length >= 1) args(0).toString else "CrimeIncidents"
    val master = if (args.length >= 2) args(1).toString else "local"
    val filePath = if (args.length >= 3) args(2).toString else "./"

    println(s"AppName=$appName")
    println(s"Master=$master")
    println(s"FilePath=$filePath")

    val spark = SparkSession.builder.
      master(master).
      appName(appName).
      getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("Question A\n")
    // a. Use Spark to truncate the file and only keep these 6 items of each line of record.
    val crimesDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filePath + "Crime_Incidents_in_2013.csv")

    val selectedColumn = crimesDF.select("CCN", "REPORT_DAT", "OFFENSE", "METHOD", "END_DATE", "DISTRICT")
    selectedColumn.printSchema()

    // b. Use Spark queries to count the number of each type offenses.
    println("Question B\n")
    println("Count the number of each type offenses:")
    selectedColumn.groupBy("OFFENSE").count()
      .orderBy(desc("count"))
      .collect().foreach(r => println(r.mkString("\t")))

    println("\nWhich time-slot (shift) did the most crimes occur:")
    val offencesDF = crimesDF.select("CCN", "REPORT_DAT", "SHIFT", "OFFENSE", "METHOD", "END_DATE", "DISTRICT")
    offencesDF.groupBy("SHIFT").count()
      .orderBy(desc("count"))
      .take(1).foreach(r => println(r.mkString("\t")))

    val firstGroup = offencesDF.groupBy("OFFENSE", "SHIFT").count().alias("count")
    val secondGroup = firstGroup.groupBy("OFFENSE").agg(max("count").alias("max"))
        .orderBy(desc("max"))
    val joinedResult = firstGroup.join(secondGroup, "OFFENSE")
        .where("max == count")
        .select("OFFENSE", "SHIFT", "count")
        .orderBy(desc("count"))

    println("\nFor each crime methods, the most shift occur:")
    joinedResult.collect().foreach(r => println(r.mkString("\t")))

    println("\nQuestion C")
    var totalCrimes = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filePath + "Crime_Incidents_in_*.csv")
      .select("REPORT_DAT", "METHOD")
      .where("METHOD == 'GUN'")

    totalCrimes = totalCrimes.withColumn("year", year(totalCrimes("REPORT_DAT")))
      .groupBy("year")
      .agg(count("METHOD").alias("count"))
      .withColumn("fraction", col("count") / sum("count").over())

    println("Compute the percentage of gun offense for each year:")
    totalCrimes.select("year", "fraction")
      .orderBy(asc("year"))
      .collect().foreach(r => println(r.mkString("\t")))

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println(s"Total time cost=${durationSeconds}s")

  }

}
