package com.spark.coding.challenge

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Schemas {

    val sales: StructType = StructType(Array(
      StructField("PRODUCT_UPC", StringType, nullable = true),
      StructField("TRACK_ISRC_CODE", StringType, nullable = true),
      StructField("TRACK_ID", StringType, nullable = true),
      StructField("DELIVERY", StringType, nullable = true),
      StructField("NET_TOTAL", DoubleType, nullable = true),
      StructField("TERRITORY", StringType, nullable = true)
    ))

    val songs: StructType = StructType(Array(
      StructField("isrc", StringType, nullable = true),
      StructField("song_id", StringType, nullable = true),
      StructField("song_name", StringType, nullable = true),
      StructField("artist_name", StringType, nullable = true),
      StructField("content_type", StringType, nullable = true)
    ))

    val albums: StructType = StructType(Array(
      StructField("upc", StringType, nullable = true),
      StructField("album_name", StringType, nullable = true),
      StructField("label_name", StringType, nullable = true),
      StructField("country", StringType, nullable = true)
    ))
}
