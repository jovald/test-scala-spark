package com.jovald.spark

import org.apache.spark._
import org.apache.log4j._

object WordCount {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "MinimunTemperatureByLocation")

    val lines = sc.textFile("src/assets/book.txt")

    val countWords = lines.flatMap(_.split("\\W+"))
      .map(_.toLowerCase())
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey()
      .collect()

    countWords.foreach(println)

  }

}
