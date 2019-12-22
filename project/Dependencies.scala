import sbt._

object Dependencies {
  def flink(flinkVersion: String): Seq[ModuleID] = Seq(
    "org.apache.flink" % "flink-core" % flinkVersion,
  )
}