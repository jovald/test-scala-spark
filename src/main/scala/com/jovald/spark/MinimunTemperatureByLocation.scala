package com.jovald.spark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min

object MinimunTemperatureByLocation {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "MinimunTemperatureByLocation")

    val lines = sc.textFile("src/assets/1800.csv")

    val minimumTemperatureByLocation = lines.map(parseLine)
      .filter(_._2 == "TMIN")
      .map(x => (x._1, x._3.toFloat))
      .reduceByKey((x, y) => min(x, y))
      .collect()
      .sorted

    minimumTemperatureByLocation.foreach(println)

  }

}
