import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appVersion = "0.1"

  val common = Project(
      "satisfaction-common",
      file("modules/common")
  ).settings(CommonSettings: _*).settings(version := appVersion)


  val executor = Project(
      "executor",
      file("modules/executor")
  ).settings(CommonSettings: _*).settings(version := appVersion).dependsOn(common)

  val samples = Project(
      "satisfaction-samples",
      file("modules/samples")
  ).settings(CommonSettings: _*).settings(version := appVersion).dependsOn(common, executor)


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

  val hiveVersion = "0.10.0-cdh4.2.1-p98.51"
  ///val hiveVersion = "0.11.0"

  def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) = 
    items.map(_.exclude(group, artifact))

  implicit def dependencyFilterer(deps: Seq[ModuleID]) = new Object {
		    def excluding(group: String, artifactId: String) =
			    deps.map(_.exclude(group, artifactId))

		    def excludingGroup(group: String) =
			    deps.map(_.exclude(group, "*"))
  }


  def Dependencies = libraryDependencies ++= Seq(
      jdbc,
      anorm,
	  ("org.apache.hive" % "hive-common" % hiveVersion),
	  ("org.apache.hive" % "hive-exec" % hiveVersion),
	  ("org.apache.hive" % "hive-metastore" % hiveVersion),
	  ("org.apache.hive" % "hive-cli" % hiveVersion),
	  ("org.apache.hive" % "hive-serde" % hiveVersion),
	  ("org.apache.hive" % "hive-shims" % hiveVersion),
	  ("org.apache.hive" % "hive-hbase-handler" % hiveVersion),
	  ("org.apache.hive" % "hive-jdbc" % hiveVersion),
	  ("org.apache.hive" % "hive-service" % hiveVersion ),
	  ("org.apache.hive" % "hive-builtins" % "0.10.0"),
	  ("org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-hdfs" % "2.0.0-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-tools" % "2.0.0-mr1-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.0.0-cdh4.2.1"),
	  ("org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.2.1"),
	  ("javax.jdo" % "jdo-api" % "3.0.1"),
	  ("mysql" % "mysql-connector-java" % "5.1.18" ),
    ("com.github.nscala-time" %% "nscala-time" % "0.4.2"),
	("com.inadco.ecoadapters" % "ecoadapters" % "0.4.3-klout"),
	("org.elasticsearch" % "elasticsearch-hadoop" % "0.0.1.klout").exclude("cascading" , "*"),
	("com.klout.pipeline" % "platform-protos" % "0.91.6").exclude("com.googlecode.protobuf-java-format","protobuf-java-format"),
	("com.googlecode.protobuf-java-format" % "protobuf-java-format" % "1.2"),

      ("com.klout" %% "scoozie" % "0.5.2" ).exclude("org.apache.hive","*"),
      ("com.klout.scoozie" %% "klout-scoozie-maxwell" % "0.4" % "compile").exclude("org.apache.hadoop.hive","*"),
      ("com.klout.scoozie" %% "klout-scoozie-common" % "0.5" % "compile").exclude("org.apache.hive","*"),
	  ("com.googlecode.protobuf-java-format" % "protobuf-java-format" % "1.2"),
	  ("org.specs2" %% "specs2" % "1.14" % "test"),
    ("us.theatr" %% "akka-quartz" % "0.2.0"),
	  ("org.apache.thrift" % "libfb303" % "0.7.0" ),
	  ("org.antlr" % "antlr-runtime" % "3.4" )

  ).excluding("org.apached.hadoop.hive","hive-cli").excluding("javax.jdo","jdo2-api").excluding("commons-daemon","commons-daemon").
     excluding("org.apache.hbase","hbase").excluding("org.apache.maven.wagon","*")


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
