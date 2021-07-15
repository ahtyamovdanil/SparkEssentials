package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, desc, mean, min, stddev, sum}

object Aggregations extends App {

    val spark = SparkSession.builder()
      .appName("Aggregations and grouping")
      .master("local")
      .getOrCreate()

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

    // counting
    val genresCountDF = moviesDF.select(count(col("Major_Genre"))) //all values except nulls
    moviesDF.selectExpr("count(Major_Genre)") // the same
    moviesDF.select(count("*")) // count all rows in the original dataframe

    // counting distinct
    moviesDF.select(countDistinct(col("Major_Genre")))

    // approximate count
    moviesDF.select(approx_count_distinct(col("Major_Genre")))

    // min and max
    val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
    moviesDF.selectExpr("min(IMDB_Rating)")

    // sum
    moviesDF.select(sum("US_Gross"))
    moviesDF.selectExpr("sum(US_Gross)")

    // avg
    moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
    moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

    // data science
    moviesDF.select(
        mean(col("Rotten_Tomatoes_Rating")).as("mean"),
        stddev(col("Rotten_Tomatoes_Rating")).as("std")
    ).show()

    // grouping
    val countByGenreDF = moviesDF
      .groupBy(col("Major_Genre")) // includes nulls
      .count() //select count(*) from DF group by Major_Genre

    val avgRatingByGenreDf = moviesDF
      .groupBy(col("Major_Genre"))
      .avg("IMDB_Rating")

    val aggregationsByGenre = moviesDF
      .groupBy(col("Major_Genre"))
      .agg(
          count("*").as("N_movies"),
          avg("IMDB_Rating").as("Avg_Rating"),
          stddev("IMDB_Rating").as("Std_Rating")
      )
      .orderBy(col("Avg_Rating"))

    /**
      * Exercises
      * 1. Sum up ALL profits of ALL the movies in the DF
      * 2. Count how many distinct directors we have
      * 3. Show the mean and standard deviation of US gross revenue for the movies
      * 4. Compute avg IMDB rating and the avg US gross PER DIRECTOR
      */

    // 1
    moviesDF
      .select((col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
      .select(sum("Total_Gross")).show()

    // 2
    moviesDF.select(countDistinct("Director")).show()

    // 3
    moviesDF.select(
        avg("US_Gross").as("mean_gross"),
        stddev("US_Gross").as("std_gross")
    )

    // 4
    moviesDF.groupBy(col("Director"))
      .agg(
          avg("IMDB_Rating").as("mean_rating"),
          sum("US_Gross").as("total_us_gross")
      )
      .sort(col("mean_rating").desc_nulls_last)
      .show()
}
