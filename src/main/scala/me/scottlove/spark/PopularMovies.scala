
package me.scottlove.spark

import org.apache.spark._
import org.apache.log4j._


object PopularMovies {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMovies")

    val lines = sc.textFile("../ml-100k/u.data")

    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    val movieCounts = movies.reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(false)
      .map(_.swap)

    val results = movieCounts.collect()
    results.foreach(println)
  }

}