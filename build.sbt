name := "flink-btree"

version := "0.1"

scalaVersion := "2.12.7"

val flinkVersion = "1.9.0"

lazy val root = (project in file(".")).settings(
  libraryDependencies ++= (
    Dependencies.flink(flinkVersion)
    )
)
