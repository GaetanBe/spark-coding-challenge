name := "ScalaSparkExample"
version := "0.0.1"
scalaVersion := "2.13.14"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  //  Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Additional dependencies
  "io.delta" %% "delta-core" % "2.4.0",
  "com.github.pureconfig" %% "pureconfig" % "0.17.6",
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.5.3" % "test",
)

Test / fork := true
Test / parallelExecution := false
Test / javaOptions ++= Seq(
  "-Xms2G",
  "-Xmx2G"
)

Compile / mainClass := Some("com.spark.coding.challenge.SparkMain")
