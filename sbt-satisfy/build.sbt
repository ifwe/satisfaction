
sbtPlugin := true

name := "sbt-satisfy"

scalaVersion := "2.10.2"

organization := "com.tagged.satisfaction"

version := "0.12"

val hadoopVersion = "2.3.0"

libraryDependencies ++= Seq(
  ("org.apache.hadoop" % "hadoop-common" % hadoopVersion),
  ("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion)
)

publishMavenStyle := false

publishTo := Some("tagged-artifactory-release" at "http://artifactory.tagged.com:8081/artifactory/libs-release-local")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")


