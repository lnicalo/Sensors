import scoverage.ScoverageSbtPlugin

name := "Sensors"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0"
  // "org.apache.hbase" % "hbase-client" % "2.0.0-SNAPSHOT",
  // "org.apache.hbase" % "hbase-server" % "2.0.0-SNAPSHOT",
  // "org.apache.hbase" % "hbase-common" % "2.0.0-SNAPSHOT",
  // "org.apache.hbase" % "hbase-spark" % "2.0.0-SNAPSHOT",
  //"it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"
)

CoverallsPlugin.coverallsSettings

resolvers += "Apache Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots"
