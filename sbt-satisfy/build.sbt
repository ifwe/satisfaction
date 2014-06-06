
sbtPlugin := true

name := "sbt-satisfy"

scalaVersion := "2.10.2"

organization := "com.tagged.satisfaction"

version := "0.1"

val hadoopVersion = "2.3.0"

libraryDependencies ++= Seq(
  ("org.apache.hadoop" % "hadoop-common" % hadoopVersion),
  ("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion)
)

