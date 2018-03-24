package com.jovald.spark

import org.apache.spark._
import org.apache.log4j._

object AmountByCostumer {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val customer = fields(0).toInt
    val amount = fields(2).toFloat
    (customer, amount)
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "AmountByCostumer")

    val lines = sc.textFile("src/assets/customer-orders.csv")

    val totalAmountByCostumer = lines.map(parseLine)
      .reduceByKey((x, y) => x + y)
        .map(x => (x._2, x._1))
      .sortByKey()
      .collect()


    totalAmountByCostumer.foreach(println)
  }

}
