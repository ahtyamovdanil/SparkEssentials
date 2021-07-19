package part2dataframes.Salting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, explode, monotonically_increasing_id, typedLit}

object KeySalting2 extends App {

    val spark = SparkSession.builder()
      .master("local")
      .appName("joins")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val guitarsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/guitars.json")

    val guitaristsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/guitarPlayers.json")

    val bandsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/bands.json")


//    guitarsDF.show(false)
//    guitaristsDF.show(false)
//    bandsDF.show(false)

    val guitaristsDfSalted = guitaristsDF
      .withColumn("guitar", explode(col("guitars")))
      .drop("guitars")
      .withColumn("dummy", monotonically_increasing_id % 2)
      .withColumn("guitar_id_salt", concat_ws("-", col("guitar"), col("dummy")))
      .drop("dummy")

    val guitarsDfSalted = guitarsDF
      .withColumn("dummy", typedLit((0 to 1).toList))
      .withColumn("salt", explode(col("dummy")))
      .withColumn("id_salt", concat_ws("-", col("id"), col("salt")))
      .drop("dummy", "salt")

    guitaristsDF.withColumn("guitar_id", explode(col("guitars"))).join(
        guitarsDF.withColumnRenamed("id", "guitar_id"), Seq("guitar_id"), "inner")
      .show()

    guitaristsDfSalted.join(guitarsDfSalted, col("guitar_id_salt") === col("id_salt"), "inner").show()
    //guitaristsDfSalted.show()
    //guitarsDfSalted.show()


}
