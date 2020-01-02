import sbt._

object Dependencies {
  def flink(flinkVersion: String): Seq[ModuleID] = Seq(
    "org.apache.flink" % "flink-core" % flinkVersion,
    "org.apache.flink" %% "flink-runtime" % flinkVersion % "test" classifier "tests",
    "org.apache.flink" %% "flink-streaming-java" % flinkVersion % "test" classifier "tests",
    "org.apache.flink" %% "flink-test-utils" % flinkVersion % "test"
  )

  def scalaTest(scalaTestVersion: String): Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  )
}