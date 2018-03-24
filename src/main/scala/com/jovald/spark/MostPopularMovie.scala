package com.jovald.spark

import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object MostPopularMovie {

  def loadMovieNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("src/assets/ml-100k/u.item").getLines()

    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  def parseLine(line: String) = {
    val fields = line.split("\t")
    val movie = fields(1).toInt
    (movie, 1)
  }

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "MostPouplarMovie")

    var nameDict = sc.broadcast(loadMovieNames)

    val lines = sc.textFile("src/assets/ml-100k/u.data")

    val mostPopularMovie = lines.map(parseLine)
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (nameDict.value(x._2), x._1))
      .collect()

    mostPopularMovie.foreach(println)

  }

}
