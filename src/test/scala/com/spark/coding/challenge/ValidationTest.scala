package com.spark.coding.challenge

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.spark.coding.challenge.DataIssuesHandler.Status.Status
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite

class ValidationTest extends AnyFunSuite with DataFrameSuiteBase {

  import spark.implicits._

  def mockDataIssueHandler(issue: DataIssuesHandler.DataIssueToHandle): Unit = {}

  test("validate should correctly split valid and invalid rows and handle data issues") {
    val sales = Seq(
      ("track1", "song1", "upc1", "US"),
      (null, "song2", "upc2", "UK")
    ).toDF("TRACK_ISRC_CODE", "TRACK_ID", "PRODUCT_UPC", "TERRITORY")

    val songs = Seq(
      ("isrc1", "song1"),
      ("isrc2", null)
    ).toDF("isrc", "song_id")

    val albums = Seq(
      ("upc1", "US"),
      (null, "UK")
    ).toDF("upc", "country")

    val inputs = Reader.Inputs(sales, songs, albums)

    val expectedValidSales = Seq(
      ("track1", "song1", "upc1", "US")
    ).toDF("TRACK_ISRC_CODE", "TRACK_ID", "PRODUCT_UPC", "TERRITORY")

    val expectedValidSongs = Seq(
      ("isrc1", "song1")
    ).toDF("isrc", "song_id")

    val expectedValidAlbums = Seq(
      ("upc1", "US")
    ).toDF("upc", "country")

    implicit val dataIssueHandler: ((String, DataFrame, Status)) => Unit = mockDataIssueHandler
    val result: Reader.Inputs = Validation.validate(inputs)

    assertDataFrameEquals(expectedValidSales, result.sales)
    assertDataFrameEquals(expectedValidSongs, result.songs)
    assertDataFrameEquals(expectedValidAlbums, result.albums)
  }
}
