package com.spark.coding.challenge

import pureconfig.ConfigSource
import pureconfig.generic.auto._

object Config {

    final case class MainSparkConfig(
        salesPath: String,
        songsPath: String,
        albumsPath: String,
        outputTable: String,
        handlerUrl: String,
    )

    def getConfig: MainSparkConfig = ConfigSource
        .default
        .load[MainSparkConfig]
        // Make program fail if configuration is not properly loaded
        .getOrElse(throw new Exception("Failed to load configuration"))

}
