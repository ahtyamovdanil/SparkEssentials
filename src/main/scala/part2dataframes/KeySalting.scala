package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KeySalting extends App {

    val spark = SparkSession.builder()
      .master("local")
      .appName("joins")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val cars = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/cars.json")

    val carsMain = cars.select("Origin", "Year", "Miles_per_Gallon", "Displacement", "Weight_in_lbs")
    val carsSecond = cars.select("Origin", "Name")

    val carsMainSalted = carsMain
      .withColumn("dummy", monotonically_increasing_id % 20)
      .withColumn("id_salt", concat_ws("-", col("Origin"), col("dummy")))
      .drop("dummy")


    val carsSecondSalted = carsSecond
      .withColumn("all", typedLit((0 to 20).toList))
      .withColumn("salt", explode(col("all")))
      .withColumn("id_salt", concat_ws("-", col("Origin"), col("salt")))
      .drop("all", "salt", "Origin")

    val resultSalted = carsSecondSalted
      .join(carsMainSalted, Seq("id_salt"))

    cars.join(cars.drop("Miles_per_Gallon", "Name"), Seq("Origin"), "inner").groupBy("Name").sum("Miles_per_Gallon").show()
    resultSalted.groupBy("Name").sum("Miles_per_Gallon").show()
    //carsMain.join(carsSecond, Seq("Origin")).show()

}
