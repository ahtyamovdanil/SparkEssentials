package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

    val spark = SparkSession.builder()
      .appName("DF columns and expressions")
      .config("spark.master", "local")
      .getOrCreate()

    val carsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/cars.json")

    val firstColumn = carsDF.col("Name")

    // selecting
    val carNames = carsDF.select(firstColumn)

    // select multiple columns
    val multipleColumns = carsDF.select(
        carsDF.col("Name"),
        carsDF.col("Acceleration")
    )

    // another approach, using spark.sql.functions. col or column
    carsDF.select(
        col("Name"),
        column("Acceleration")
    )

    // and another way, using implicits

    import spark.implicits._

    carsDF.select(
        'Name,
        'Acceleration, // scala symbol, auto-converted to column
        $"Horsepower", //fancier interpolated string, returns a Column object
        expr("origin") // EXPRESSION
    )

    // or just use strings
    carsDF.select("Name", "Acceleration", "Origin")

    // EXPRESSIONS
    val simplestExpression = carsDF.col("Weight_in_lbs")
    val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

    val carsWithWeightsDF = carsDF.select(
        col("Name"),
        col("Weight_in_lbs"),
        weightInKgExpression.as("Weight_in_kg"),
        expr("Weight_in_lbs / 2.2").as("Weight_in_kg2") //another way (but expr has different implementation)
    )

    val carsWithSelectExprWeights = carsDF.selectExpr(
        "Name",
        "Weight_in_lbs",
        "Weight_in_lbs / 2.2"
    )

    // DF processing

    // add a new column
    val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
    // rename a column
    val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
    // careful with column names
    carsWithColumnRenamed.selectExpr("`Weight in pounds`") // use ` if column name has multiple words
    //remove column
    carsWithColumnRenamed.drop("Cylinders", "Displacement")
    //filtering
    val europeanCarsDF = carsDF.filter(col("origin") =!= "USA") // use =!= instead of !=
    val europeanCardsDF2 = carsDF.where(col("origin") =!= "USA")
    val americanCarsDF = carsDF.where(col("origin") === "USA")
    // filtering with expression strings
    val americanCarsDF2 = carsDF.filter("Origin = 'USA'")

    // chain filters
    val americanPowerfulCarsDF = carsDF
      .filter(col("Origin") === "USA")
      .filter(col("Horsepower") > 150)

    val americanPowerfulCarsDF2 = carsDF.filter(
        col("Origin") === "USA" and
          col("Horsepower") > 150
    )

    val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

    // unioning = adding more rows
    val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
    val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

    // distinct
    val allCountriesDF = carsDF.select("Origin").distinct()
    //allCountriesDF.show()

    /**
      * Exercises
      * 1. Read the movies dataframe and select 2 columns of your choice
      * 2. Create another column by selecting total gross = US_gross + Worldwide_Gross
      * 3. Select all Comedy columns ("Major_Genre") and "IMDB_Rating" > 6
      *
      * Use as many versions as possible
      *
      */

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

    // 1
    moviesDF.select("Title", "US_Gross")

    // 2
    moviesDF.withColumn("Total_Gross", expr("US_gross + Worldwide_Gross"))
    moviesDF.selectExpr("US_gross + Worldwide_Gross")
    moviesDF.select(col("US_gross") + col("Worldwide_Gross"))
    moviesDF.selectExpr("US_gross", "Worldwide_Gross", "US_DVD_Sales", "US_gross + Worldwide_Gross as Total_Gross").show()

    //3
    moviesDF.select("Title", "IMDB_Rating")
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

    moviesDF.select("Title", "IMDB_Rating").filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")


}
