
import sbt._
import Keys._

import play.Project._

object ApplicationBuild extends Build {

  val appVersion = "0.3.1"

  ///val hiveVersion = "0.10.0-cdh4.3.2"
  val hiveVersion = "0.10.0-cdh4.2.1-p98.51"
  ///val hiveVersion = "0.11.0"

  val core = Project(
      "satisfaction-core",
      file("modules/core")
  ).settings(CommonSettings: _* ).settings(libraryDependencies := coreDependencies)

  val engine = Project(
      "satisfaction-engine",
      file("modules/engine")
  ).settings(CommonSettings: _*).settings( libraryDependencies := engineDependencies ).dependsOn(core)

  val hadoop = Project(
      "satisfaction-hadoop",
      file("modules/hadoop")
  ).settings(CommonSettings: _*).settings(libraryDependencies := hadoopDependencies ).dependsOn(core).dependsOn( engine)

  val hive = Project(
      "satisfaction-hive",
      file("modules/hive")
  ).settings(CommonSettings: _*).settings(libraryDependencies  := hiveDependencies ).dependsOn(core).dependsOn(hadoop).dependsOn( engine)

  val scoozie = Project(
      "satisfaction-scoozie",
      file("modules/scoozie")
  ).settings(CommonSettings: _*).settings(libraryDependencies := scoozieDependencies ).dependsOn(core).dependsOn(hadoop)

  val packaging = Project(
     "satisfaction-packaging",
     file("modules/packaging")
  ).settings(CommonSettings: _*).settings(sbtPlugin := true)

  val willrogers = play.Project(
      "willrogers",
      appVersion,
      path = file("apps/willrogers")
  ).settings(CommonSettings: _*).dependsOn(core, engine, hadoop, hive)

  def CommonSettings =  Resolvers ++ Seq(
      scalacOptions ++= Seq(
          "-unchecked",
          "-feature",
          "-deprecation",
          "-language:existentials",
          "-language:postfixOps",
          "-language:implicitConversions",
          "-language:reflectiveCalls"
      ),

      scalaVersion := "2.10.2",

	  organization := "com.klout.satisfaction",

	  version := appVersion,
     
      libraryDependencies ++= testDependencies

  )

  def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) = 
    items.map(_.exclude(group, artifact))

  implicit def dependencyFilterer(deps: Seq[ModuleID]) = new Object {
		    def excluding(group: String, artifactId: String) =
			    deps.map(_.exclude(group, artifactId))

		    def excludingGroup(group: String) =
			    deps.map(_.exclude(group, "*"))
  }


 
  def testDependencies = Seq(
    ("org.specs2" %% "specs2" % "1.14" % "test"),
    ("junit" % "junit" % "4.11" % "test")
  )


  def hadoopDependencies = Seq(
	  ("org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-hdfs" % "2.0.0-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-tools" % "2.0.0-mr1-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.0.0-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-lzo" % "0.4.10") 
  ).excluding("commons-daemon", "commons-daemon" ) ++ testDependencies ++ metastoreDependencies

  def coreDependencies = Seq(
    ("com.github.nscala-time" %% "nscala-time" % "0.4.2")
  ) ++ testDependencies 

  def metastoreDependencies = Seq(
	  ("org.apache.hive" % "hive-common" % hiveVersion),
	  ("org.apache.hive" % "hive-shims" % hiveVersion),
	  ("org.apache.hive" % "hive-metastore" % hiveVersion),
	  ("org.apache.hive" % "hive-exec" % hiveVersion),
	  ("org.apache.hive" % "hive-builtins" % hiveVersion),
	  ("org.apache.thrift" % "libfb303" % "0.7.0" )
  )

  def hiveDependencies = Seq(
	  ("org.apache.hive" % "hive-common" % hiveVersion),
	  ("org.apache.hive" % "hive-exec" % hiveVersion),
	  ("org.apache.hive" % "hive-metastore" % hiveVersion),
	  ("org.apache.hive" % "hive-cli" % hiveVersion),
	  ("org.apache.hive" % "hive-serde" % hiveVersion),
	  ("org.apache.hive" % "hive-shims" % hiveVersion),
	  ("org.apache.hive" % "hive-hbase-handler" % hiveVersion),
	  ("org.apache.hive" % "hive-jdbc" % hiveVersion),
	  ("org.apache.hive" % "hive-service" % hiveVersion ),
	  ("org.apache.thrift" % "libfb303" % "0.7.0" ),
	  ("org.antlr" % "antlr-runtime" % "3.4" ),
	  ("org.antlr" % "antlr" % "3.0.1" )
  ) ++ metastoreDependencies ++ testDependencies

  def scoozieDependencies = Seq(
     ("com.klout" %% "scoozie" % "0.5.3" ).exclude("org.apache.hive","*")
  )


  def engineDependencies = Seq(
    ("com.typesafe.akka" %% "akka-actor" % "2.1.0"),
    ("us.theatr" %% "akka-quartz" % "0.2.0")
  ) ++ testDependencies


  def Dependencies = libraryDependencies ++= Seq(
      jdbc,
      anorm,
	  ("javax.jdo" % "jdo-api" % "3.0.1"),
	  ("mysql" % "mysql-connector-java" % "5.1.18" ),
    ("com.github.nscala-time" %% "nscala-time" % "0.4.2"),
	("com.googlecode.protobuf-java-format" % "protobuf-java-format" % "1.2"),

	  ("com.googlecode.protobuf-java-format" % "protobuf-java-format" % "1.2"),
	  ("org.specs2" %% "specs2" % "1.14" % "test"),
    ("us.theatr" %% "akka-quartz" % "0.2.0"),
	  ("org.apache.thrift" % "libfb303" % "0.7.0" ),
	  ("org.antlr" % "antlr-runtime" % "3.4" ),
	  ("org.antlr" % "antlr" % "3.0.1" ),

	  ( "org.scala-lang" % "scala-reflect" % "2.10.2" )



  ).excluding("org.apached.hadoop.hive","hive-cli").excluding("javax.jdo","jdo2-api").excluding("commons-daemon","commons-daemon").
     excluding("org.apache.hbase","hbase").excluding("org.apache.maven.wagon","*")


  def Resolvers = resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
	  /// XXX Remove klout dependencies
      "Klout Maven libs Repository" at "http://maven-repo:8081/artifactory/libs-release",
      "Klout Remote Repositories" at "http://maven-repo:8081/artifactory/remote-repos",
      "Klout Maven external libs Repository" at "http://maven-repo:8081/artifactory/ext-release-local",
      "Klout Maven external snapshots Repository" at "http://maven-repo:8081/artifactory/ext-snapshot-local",
      "local-maven-repo-releases" at "http://maven-repo:8081/artifactory/libs-release-local",
      "local-maven-repo-snapshots" at "http://maven-repo:8081/artifactory/libs-snapshot-local/",
      "theatr" at "http://repo.theatr.us"
  )

}
