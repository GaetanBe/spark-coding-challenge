package com.spark.coding.challenge

import com.spark.coding.challenge.Config.MainSparkConfig
import com.spark.coding.challenge.DataIssuesHandler.DataIssueToHandle
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkMain {
    def main(args: Array[String]): Unit = {

        val config: MainSparkConfig = Config.getConfig

        val spark: SparkSession = SparkSession.builder()
            .appName("Data Ingestion")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()

        implicit val dataIssuesHandler: DataIssueToHandle => Unit =
            DataIssuesHandler.handleDataIssues(config.handlerUrl)

        val inputs: Reader.Inputs = Reader.readInputs(config, spark)
        val validatedInputs: Reader.Inputs = Validation.validate(inputs)
        val output: DataFrame = Hydration.hydrate(validatedInputs)

        val result: DataFrame = output.select(
            col("upc"),
            col("isrc"),
            col("label_name"),
            col("album_name"),
            col("song_id"),
            col("song_name"),
            col("artist_name"),
            col("content_type"),
            col("NET_TOTAL").as("total_net_revenue"),
            col("TERRITORY").as("sales_country"),
        )

        result.write.format("delta").mode("append").save(config.outputTable)
        spark.stop()
    }
}
