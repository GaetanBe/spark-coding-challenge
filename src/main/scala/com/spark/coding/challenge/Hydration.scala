package com.spark.coding.challenge

import com.spark.coding.challenge.DataIssuesHandler.{DataIssueToHandle, Status}
import org.apache.spark.sql.DataFrame

object Hydration {

    def hydrate(inputs: Reader.Inputs)(implicit dataIssuesHandler: DataIssueToHandle => Unit): DataFrame = {
        val salesSongs: DataFrame = inputs.sales
            .join(
                inputs.songs,
                inputs.sales("TRACK_ISRC_CODE") === inputs.songs("isrc")
                    && inputs.sales("TRACK_ID") === inputs.songs("song_id"),
                "left_outer",
            )

        val output: DataFrame = salesSongs
            .join(
                inputs.albums,
                salesSongs("PRODUCT_UPC") === inputs.albums("upc")
                    && salesSongs("TERRITORY") === inputs.albums("country"),
                "left_outer",
            )

        val issuesToHandle: Seq[DataIssueToHandle] = Seq(
            (
                "filteredOutSalesSongs",
                salesSongs.filter(salesSongs("isrc").isNull || salesSongs("song_id").isNull),
                Status.OrphanInput,
            ),
            (
                "filteredOutOutput",
                output.filter(output("upc").isNull || output("country").isNull),
                Status.OrphanInput,
            ),
        )

        issuesToHandle.foreach(dataIssuesHandler)

        output
    }
}
