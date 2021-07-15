package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {

    val spark = SparkSession.builder()
      .master("local")
      .appName("joins")
      .getOrCreate()

    val guitarsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/guitars.json")

    val guitaristsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/guitarPlayers.json")

    val bandsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/bands.json")

    // inner joins
    val guitaristsBandsDF = guitaristsDF.join(bandsDF, guitaristsDF.col("band") === bandsDF.col("id"), "inner")

    val joinCondition = guitaristsBandsDF.col("band") === bandsDF.col("id")
    val guitaristsBandsDF2 = guitaristsDF.join(bandsDF, joinCondition, "inner")

    // outer joins
    // left outer = everything in the inner join + all the rows from the LEFT table, with nulls in where data is missing
    guitaristsDF.join(bandsDF, joinCondition, "left_outer")

    // right outer = everything in the inner join + all the rows from the RIGHT table, with nulls in where data is missing
    guitaristsDF.join(bandsDF, joinCondition, "right_outer")

    // full outer join = everything in the inner join + all the rows from the BOTH table, with nulls in where data is missing
    guitaristsDF.join(bandsDF, joinCondition, "outer")

    // semi-joins = inner without columns from the right table
    guitaristsDF.join(bandsDF, joinCondition, "left_semi")

    // ani-joins = inner (only left columns) without rows that satisfy the condition
    guitaristsDF.join(bandsDF, joinCondition, "left_anti")

    // things to bear in mind
    // guitaristsBandsDF.select("id", "band").show() // this crashes

    // option 1 - rename the column which we are joining
    guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

    // option 2 - drop the dupe column
    guitaristsDF.drop(bandsDF.col("id"))

    // option 3 - rename the offending column and keep the data
    val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
    guitaristsDF.join(bandsDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

    // using complex types
    guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))



}
