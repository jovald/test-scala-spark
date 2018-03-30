package com.jovald.spark

import org.apache.log4j._
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql._
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions.explode

object MovieRecommendationALS {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split('\t')
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  case class Movie(movieId: Int, movieName: String)
  def parseMovieName(str: String): Movie = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val fields = str.split('|')
    Movie(fields(0).toInt, fields(1).toString)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MovieRecommendationsALS")
      .master("local[*]")
      .getOrCreate()

    println("Loading rating data...")
    import spark.implicits._
    val ratings = spark.read.text("src/assets/ml-100k/u.data").as[String]
      .map(parseRating)
      .toDF()

    println("Loading movie names...")
    val movies = spark.read.text("src/assets/ml-100k/u.item").as[String]
      .map(parseMovieName)
      .toDF()
      .cache()

    // Build the recommendation model using ALS on the training data
    println("Training model...")
    val als = new ALS()
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setRank(8)
      .setMaxIter(8)
      .setRegParam(0.01)
    val model = als.fit(ratings)

    val testUserId = 0
    val testUserMovies = ratings.filter($"userId" === testUserId)
      .join(movies, ratings("movieID") === movies("movieId"))
      .select($"movieName")

    println("Test user movies")
    testUserMovies.show(false)

    println("Recommendations: ")
    val testUser = ratings.filter($"userId" === testUserId)

    val RecsMoviesId = model.recommendForUserSubset(testUser, 10)
      .filter($"userId" === testUserId)
      .select($"recommendations")
      .select(explode($"recommendations").as("recommendations"))
      .select($"recommendations.movieId")


    val userRecs = RecsMoviesId.join(movies, RecsMoviesId("movieID") === movies("movieId"))
      .select($"movieName")

    userRecs.show(false)


  }

}
