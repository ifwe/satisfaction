
import sbt._
import Keys._
import com.typesafe.sbt.packager.linux.{LinuxPackageMapping, LinuxSymlink}
import com.typesafe.sbt.packager.rpm.RpmDependencies

import com.typesafe.sbt.packager.Keys._
import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._

import com.typesafe.sbt.packager.universal.Keys.stagingDirectory

import play.Project._

object ApplicationBuild extends Build {

  val appVersion = "2.0.1"

  val hiveVersion = "0.13.0"

  val core = Project(
      "satisfaction-core",
      file("modules/core")
  ).settings(CommonSettings: _* ).settings(libraryDependencies := coreDependencies ++ testDependencies )

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

  val willrogers = play.Project(
      "willrogers",
      appVersion,
      path = file("apps/willrogers")
  ).settings(AppSettings: _*).settings(RpmSettings: _* ).dependsOn(core, engine, hadoop, hive)

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

      packageSummary := "lowenstein",
     
      libraryDependencies ++= testDependencies

  ) 

  def AppSettings = CommonSettings ++ playScalaSettings


  val destDir = "/usr/local/satisfaction"

  def RpmSettings = packagerSettings ++ deploymentSettings ++  playScalaSettings ++  Seq(
    maintainer in Linux := "Jerome Banks jbanks@tagged.com",
    packageDescription in Linux := "Next Generation Hadoop Scheduler",

    linuxPackageMappings <++= (projectDependencyArtifacts) map { artifactSeq : Seq[Attributed[File]] =>
          artifactSeq map ( a => { packageMapping ( a.data -> s"${destDir}/lib/${a.data.name}" )  } )
    },


    linuxPackageMappings <++= (mappings in Universal) map { universalDir => 
        universalDir.filter( {  _._2.toString.startsWith("/usr/share") } ).map { packageMapping( _ ) } 
    },


    name in Rpm := "satisfaction-scheduler",
    version in Rpm := appVersion,
    rpmRelease in Rpm:= "1",
    packageSummary in Rpm := "lowenstein",
    packageSummary in Linux := "lowenstein",
    rpmVendor in Rpm := "Tagged.com",
    rpmUrl in Rpm:= Some("http:/github.com/tagged/satisfaction"),
    rpmLicense in Rpm:= Some("Apache License Version 2"),
    packageDescription in Rpm := "Next Generation Hadoop Scheduler",
    rpmGroup in Rpm:= Some("satisfaction")

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
    ("junit" % "junit" % "4.10" % "test" intransitive() ),
    ("org.specs2" %% "specs2" % "1.14" % "test"  )
  )


  def hadoopDependencies = Seq(
	  ("org.apache.hadoop" % "hadoop-common" % "2.3.0"),
	  ("org.apache.hadoop" % "hadoop-hdfs" % "2.3.0"),
	  ("org.apache.hadoop" % "hadoop-mapreduce-client-app" % "2.3.0"),
	  ("org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.3.0"),
	  ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.3.0"),
	  ("org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.3.0"),
	  ("org.apache.hadoop" % "hadoop-distcp" % "2.3.0"),
	  ("org.hamcrest" % "hamcrest-core" % "1.3"  ) ,
          ("ch.qos.logback" % "logback-classic" % "1.0.13" ),
          ("org.slf4j" % "log4j-over-slf4j" % "1.7.7" )
  ).excluding("commons-daemon", "commons-daemon" ).excluding("junit","junit").excluding("log4j", "*").excluding("org.slf4j","slf4j-log4j12") ++ testDependencies ++ metastoreDependencies

  def coreDependencies = Seq(
    ("org.slf4j" % "slf4j-api" % "1.7.7"),
    ("com.github.nscala-time" %% "nscala-time" % "1.2.0"),
    ("org.scala-lang" % "scala-library" % "2.10.2" )
  ) ++ testDependencies 

  def metastoreDependencies = Seq(
	  ("org.apache.hive" % "hive-common" % hiveVersion),
	  ("org.apache.hive" % "hive-shims" % hiveVersion),
	  ("org.apache.hive" % "hive-metastore" % hiveVersion),
	  ("org.apache.hive" % "hive-exec" % hiveVersion),
	  ("org.apache.thrift" % "libfb303" % "0.7.0" )
  ).excluding( "log4j", "*" ).excluding("org.slf4j", "*")

  def hiveDependencies = Seq(
	  ("org.apache.hive" % "hive-common" % hiveVersion),
	  ("org.apache.hive" % "hive-exec" % hiveVersion),
	  ("org.apache.hive" % "hive-metastore" % hiveVersion),
	  ("org.apache.hive" % "hive-cli" % hiveVersion),
	  ("org.apache.hive" % "hive-serde" % hiveVersion),
	  ("org.apache.hive" % "hive-shims" %   hiveVersion ),
	  ("org.apache.hive" % "hive-hbase-handler" % hiveVersion),
	  ("org.apache.hive" % "hive-jdbc" % hiveVersion),
	  ("org.apache.hive" % "hive-service" % hiveVersion ),
	  ("org.apache.thrift" % "libfb303" % "0.7.0" ),
	  ("org.antlr" % "antlr-runtime" % "3.4" ),
	  ("org.antlr" % "antlr" % "3.0.1" )
  ).excluding("log4j", "*").excluding("org.slf4j", "slf4j-log4j12")  ++ metastoreDependencies ++ testDependencies


  def engineDependencies = Seq(
    ("com.typesafe.akka" %% "akka-actor" % "2.2.0"),
    ("org.quartz-scheduler" % "quartz" % "2.2.1")
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
	  ("org.apache.thrift" % "libfb303" % "0.7.0" ),
	  ("org.antlr" % "antlr-runtime" % "3.4" ),
	  ("org.antlr" % "antlr" % "3.0.1" ),

	  ( "org.scala-lang" % "scala-reflect" % "2.10.2" ),
          ("ch.qos.logback" % "logback-classic" % "1.0.13" )



  ).excluding("org.apached.hadoop.hive","hive-cli").excluding("javax.jdo","jdo2-api").excluding("commons-daemon","commons-daemon").
     excluding("org.apache.hbase","hbase").excluding("org.apache.maven.wagon","*").excluding("log4j","*").excluding("org.slf4j","slf4j-log4j12")


  def Resolvers = resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      ////"theatr" at "http://repo.theatr.us"
      "Maven Central" at "http://repo1.maven.org/maven2",
      "Apache Maven Repository" at "http://people.apache.org/repo/m2-snapshot-repository/"

  )

}
