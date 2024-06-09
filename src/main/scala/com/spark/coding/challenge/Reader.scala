package com.spark.coding.challenge

import com.spark.coding.challenge.Config.MainSparkConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object Reader {

    case class Inputs(sales: DataFrame, songs: DataFrame, albums: DataFrame)

    def readInputs(config: MainSparkConfig, spark: SparkSession): Inputs = {
        val sales: DataFrame = spark.read
            .option("header", "true")
            .schema(Schemas.sales)
            .csv(config.salesPath)

        val songs: DataFrame = spark.read
            .option("header", "true")
            .schema(Schemas.songs)
            .csv(config.songsPath)

        val albums: DataFrame = spark.read
            .option("header", "true")
            .schema(Schemas.albums)
            .csv(config.albumsPath)

        Inputs(sales, songs, albums)
    }
}
