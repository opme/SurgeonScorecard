name := "scorecard"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

libraryDependencies += "com.typesafe" % "config" % "1.2.1"
