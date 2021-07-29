package part2dataframes.joins

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

object IterativeBroadcastJoin extends App {

    def iterativeBroadcastJoin(dfLarge: DataFrame, dfSmall: DataFrame, joinExprs: Seq[String], nChunks: Long = 10): DataFrame = {

        @tailrec
        def joinLoop(dfLarge: DataFrame, dfSmall: DataFrame, currChunkId: Long): DataFrame = currChunkId match {
            case x if x < 0 => dfLarge
            case _ =>
                val toJoinSmall = dfSmall.filter(col("chunk_id") === currChunkId)
                val nextLarge = dfLarge.join(broadcast(toJoinSmall), joinExprs, "left_outer").
                  withColumn("label", coalesce(col("label"), col("chunk_id"))).
                  drop("chunk_id")

                nextLarge.persist()
                nextLarge.head

                joinLoop(
                    nextLarge,
                    dfSmall,
                    currChunkId - 1
                )
        }

        val dfSmallChunked = dfSmall.withColumn("chunk_id", monotonically_increasing_id % nChunks)
        joinLoop(dfLarge.withColumn("label", lit(null)), dfSmallChunked, nChunks - 1)

    }

    val spark = SparkSession.builder()
      .appName("Spark Essentials Playground App")
      .config("spark.master", "local")
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

    val guitaristsFull = guitaristsDF.withColumn("guitar_id", explode(col("guitars")))

    //guitaristsFull.join(guitarsDF.withColumnRenamed("id", "guitar_id"), Seq("guitar_id")).show()

    iterativeBroadcastJoin(
        guitaristsFull,
        guitarsDF.withColumnRenamed("id", "guitar_id").select("guitar_id"),
        Seq("guitar_id"),
        3
    ).show()

    Thread.sleep(100000000)


}
