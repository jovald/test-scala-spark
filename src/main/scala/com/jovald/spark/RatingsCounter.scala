package com.jovald.spark

import org.apache.spark._
import org.apache.log4j._


object RatingsCounter {
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("src/assets/ml-100k/u.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    val ratings = lines.map(_.toString().split("\t")(2))

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)

  }
}
