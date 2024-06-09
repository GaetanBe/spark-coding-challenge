package com.spark.coding.challenge

import org.apache.spark.sql.functions.not
import org.apache.spark.sql.{Column, DataFrame}

object DataIssuesHandler {
    private type ValidDf = DataFrame
    private type InvalidDf = DataFrame

    def split_f(df: DataFrame)(condition: Column): (ValidDf, InvalidDf) = {
        val valid = df.filter(condition)
        val invalid = df.filter(not(condition))
        (valid, invalid)
    }

    def all_columns_not_null(df: DataFrame): Column = df.columns.map(df.col(_).isNotNull).reduce(_ && _)

    type DataIssueToHandle = (String, DataFrame, Status.Status)

    object Status extends Enumeration {
        type Status = Value
        val InvalidInput, OrphanInput = Value
    }

    // Dummy function to handle invalid inputs
    // In a real-world scenario, this function would implement the logic to send invalid inputs to a dead-letter queue
    // and report the issue to the data engineering team
    def handleDataIssues(handlerUrl: String)(issue: DataIssueToHandle): Unit = {
        val (name, df, status) = issue
        if (df.count() > 0)
            println(s"Invalid $name:")
            println(s"Status: $status")
            println(s"Sending data at $handlerUrl")
            df.show()
    }
}
