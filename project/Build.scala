import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appVersion = "0.1"

  val common = Project(
      "satisfaction-common",
      file("modules/common")
  ).settings(CommonSettings: _*).settings(version := appVersion)

  val samples = Project(
      "satisfaction-samples",
      file("modules/samples")
  ).settings(CommonSettings: _*).settings(version := appVersion).dependsOn(common)

  val executor = Project(
      "executor",
      file("modules/executor")
  ).settings(CommonSettings: _*).settings(version := appVersion).dependsOn(common)

  val willrogers = play.Project(
      "willrogers",
      appVersion,
      path = file("apps/willrogers")
  ).settings(CommonSettings: _*).dependsOn(common, executor,samples)

  def CommonSettings = Dependencies ++ ScalariformSettings ++ Resolvers ++ Seq(
      scalacOptions ++= Seq(
          "-unchecked",
          "-feature",
          "-deprecation",
          "-language:existentials",
          "-language:postfixOps",
          "-language:implicitConversions",
          "-language:reflectiveCalls"
      ),

      scalaVersion := "2.10.2"
  )

  def ScalariformSettings = {
      import com.typesafe.sbt.SbtScalariform._
      import scalariform.formatter.preferences._
      scalariformSettings ++ Seq(
          ScalariformKeys.preferences := FormattingPreferences().
              setPreference(AlignParameters, true).
              setPreference(IndentSpaces, 4).
              setPreference(AlignSingleLineCaseStatements, true).
              setPreference(PreserveDanglingCloseParenthesis, true).
              setPreference(PreserveSpaceBeforeArguments, true)
      )
  }

  def Dependencies = libraryDependencies ++= Seq(
      jdbc,
      anorm,
	  ("org.apache.hive" % "hive-common" % "0.10.0").exclude("javax.jdo","jdo2-api"),
	  ("org.apache.hive" % "hive-exec" % "0.10.0").exclude("javax.jdo","jdo2-api"),
	  ("org.apache.hive" % "hive-metastore" % "0.10.0").exclude("javax.jdo","jdo2-api"),
	  ("org.apache.hive" % "hive-cli" % "0.10.0").exclude("javax.jdo","jdo2-api"),
	  ("org.apache.hive" % "hive-serde" % "0.10.0").exclude("javax.jdo","jdo2-api"),
	  ("org.apache.hive" % "hive-shims" % "0.10.0").exclude("javax.jdo","jdo2-api"),
	  ("org.apache.hive" % "hive-hbase-handler" % "0.10.0").exclude("javax.jdo","jdo2-api").exclude("org.apache.maven.wagon","*"),
	  ("org.apache.hive" % "hive-jdbc" % "0.10.0").exclude("javax.jdo","jdo2-api").exclude("org.apache.maven.wagon","*"),
	  ("org.apache.hadoop" % "hadoop-common" % "2.0.2-alpha").exclude("commons-daemon","commons-daemon"),
	  ("org.apache.hadoop" % "hadoop-client" % "2.0.2-alpha").exclude("commons-daemon","commons-daemon"),
	  ("org.apache.hadoop" % "hadoop-hdfs" % "2.0.2-alpha").exclude("commons-daemon","commons-daemon"),
	  ("org.apache.hadoop" % "hadoop-tools" % "2.0.0-mr1-cdh4.2.0").exclude("commons-daemon","commons-daemon"),
	  ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.0.2-alpha").exclude("commons-daemon","commons-daemon"),
	  ("org.apache.hadoop" % "hadoop-core" % "1.2.0"),
	  ("javax.jdo" % "jdo-api" % "3.0.1"),
	  ("mysql" % "mysql-connector-java" % "5.1.18" ),
    ("com.github.nscala-time" %% "nscala-time" % "0.4.2"),
	("com.inadco.ecoadapters" % "ecoadapters" % "0.4.3-klout"),
	("org.elasticsearch" % "elasticsearch-hadoop" % "0.0.1.klout").exclude("cascading" , "*"),
	("com.klout.pipeline" % "platform-protos" % "0.91.6").exclude("com.googlecode.protobuf-java-format","protobuf-java-format"),
	("com.googlecode.protobuf-java-format" % "protobuf-java-format" % "1.2"),

      ("com.klout" %% "scoozie" % "0.5.1" ),
      ("com.klout.scoozie" %% "klout-scoozie-common" % "0.4" % "compile"),
      ("com.klout.scoozie" %% "klout-scoozie-maxwell" % "0.4" % "compile"),
	  ("org.specs2" %% "specs2" % "1.14" % "test")
	  ("com.googlecode.protobuf-java-format" % "protobuf-java-format" % "1.2"),
	  ("org.specs2" %% "specs2" % "1.14" % "test"),
    ("us.theatr" %% "akka-quartz" % "0.2.0")
  )

  def Resolvers = resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Klout Maven libs Repository" at "http://maven-repo:8081/artifactory/libs-release",
      "Klout Remote Repositories" at "http://maven-repo:8081/artifactory/remote-repos",
      "Klout Maven external libs Repository" at "http://maven-repo:8081/artifactory/ext-release-local",
      "Klout Maven external snapshots Repository" at "http://maven-repo:8081/artifactory/ext-snapshot-local",
      "local-maven-repo-releases" at "http://maven-repo:8081/artifactory/libs-release-local",
      "local-maven-repo-snapshots" at "http://maven-repo:8081/artifactory/libs-snapshot-local/",
      "theatr" at "http://repo.theatr.us"
  )

}
