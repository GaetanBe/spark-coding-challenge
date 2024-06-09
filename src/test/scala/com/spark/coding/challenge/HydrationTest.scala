package com.spark.coding.challenge

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.spark.coding.challenge.DataIssuesHandler.Status.Status
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite

class HydrationTest extends AnyFunSuite with DataFrameSuiteBase {
    import spark.implicits._

    def mockDataIssueHandler(issue: DataIssuesHandler.DataIssueToHandle): Unit = {}

    test("hydrate should correctly join inputs and handle data issues") {
        val sales = Seq(
          ("track1", "song1", "upc1", "US"),
          ("track2", "song2", "upc2", "UK")
        ).toDF("TRACK_ISRC_CODE", "TRACK_ID", "PRODUCT_UPC", "TERRITORY")

        val songs = Seq(
          ("track1", "song1"),
          ("track2", "song2")
        ).toDF("isrc", "song_id")

        val albums = Seq(
          ("upc1", "US"),
          ("upc2", "UK")
        ).toDF("upc", "country")

        val inputs = Reader.Inputs(sales, songs, albums)

        val expectedOutput = Seq(
          ("track1", "song1", "upc1", "US", "track1", "song1", "upc1", "US"),
          ("track2", "song2", "upc2", "UK", "track2", "song2", "upc2", "UK")
        ).toDF("TRACK_ISRC_CODE", "TRACK_ID", "PRODUCT_UPC", "TERRITORY", "isrc", "song_id", "upc", "country")

        implicit val dataIssueHandler: ((String, DataFrame, Status)) => Unit = mockDataIssueHandler

        val result: DataFrame = Hydration.hydrate(inputs)

        assertDataFrameEquals(expectedOutput, result)
    }
}
