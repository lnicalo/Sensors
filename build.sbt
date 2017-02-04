import scoverage.ScoverageSbtPlugin

import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import com.typesafe.sbt.SbtScalariform.scalariformSettings

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

resolvers += "Apache Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots"

CoverallsPlugin.coverallsSettings

ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 80

ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := true

scalariformSettings ++ Seq(
    ScalariformKeys.preferences := ScalariformKeys.preferences.value
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(PreserveDanglingCloseParenthesis, true)
      .setPreference(PreserveSpaceBeforeArguments, true)
      .setPreference(RewriteArrowSymbols, true)
)
