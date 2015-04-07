
sbtPlugin := true

name := "sbt-satisfy"

scalaVersion := "2.10.2"

organization := "com.tagged.satisfaction"

version := "0.15"

val hadoopVersion = "2.3.0"

libraryDependencies ++= Seq(
  ("org.apache.hadoop" % "hadoop-common" % hadoopVersion),
  ("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion)
)

publishMavenStyle := true

isSnapshot := true

publishTo := Some("tagged-artifactory-release" at "http://artifactory.tagged.com:8081/artifactory/sbt-plugins-release-local")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")


