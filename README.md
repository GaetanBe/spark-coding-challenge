# spark-coding-challenge

## Description

This project is a coding challenge for a job application. The goal is to implement a simple Spark job that reads 3 CSV 
files, cleans the data, join them accordingly and write the result to a Delta table.

It also implements a dummy way to handle malformed input rows as well as orphaned rows.

### Further considerations

In production, one must want to implement an aggregated update of that Delta table, instead of appending to it. This
would take benefit of the batch nature of this job.

Even further, one could want to implement a streaming job that would append to the Delta table in real-time, and then
run a job using Delta's API with `MERGE` to update the aggregated table.

## Running the project

Tested with OpenJDK 11

~~~
sbt test
~~~
