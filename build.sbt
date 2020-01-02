name := "flink-btree"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "1.9.0"
val scalaTestVersion = "3.1.0"

lazy val root = (project in file(".")).settings(
  libraryDependencies ++= (
    Dependencies.flink(flinkVersion)
     ++ Dependencies.scalaTest(scalaTestVersion)
  )
)
