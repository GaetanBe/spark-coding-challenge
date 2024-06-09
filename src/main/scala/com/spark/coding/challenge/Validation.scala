package com.spark.coding.challenge

import com.spark.coding.challenge.DataIssuesHandler.{DataIssueToHandle, Status, all_columns_not_null, split_f}

object Validation {

    def validate(inputs: Reader.Inputs)(implicit dataIssuesHandler: DataIssueToHandle => Unit): Reader.Inputs = {
        val (validSales, invalidSales) = split_f(inputs.sales)(all_columns_not_null(inputs.sales))
        val (validSongs, invalidSongs) = split_f(inputs.songs)(all_columns_not_null(inputs.songs))
        val (validAlbums, invalidAlbums) = split_f(inputs.albums)(all_columns_not_null(inputs.albums))

        val inputsToHandle: Seq[DataIssueToHandle] = Seq(
            ("sales", invalidSales, Status.InvalidInput),
            ("songs", invalidSongs, Status.InvalidInput),
            ("albums", invalidAlbums, Status.InvalidInput),
        )

        inputsToHandle.foreach(dataIssuesHandler)

        Reader.Inputs(validSales, validSongs, validAlbums)
    }
}
