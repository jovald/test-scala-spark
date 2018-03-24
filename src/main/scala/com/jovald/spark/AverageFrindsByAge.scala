package com.jovald.spark

import org.apache.spark._
import org.apache.log4j._

object AverageFrindsByAge {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "AverageFrindsByAge")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("src/assets/fakefriends.csv")

    // Calculating the average number of friends per age
    val averageByAge = lines.map(parseLine)
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .collect()
      .sorted

    averageByAge.foreach(println)

  }
}
