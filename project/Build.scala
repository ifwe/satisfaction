
import sbt._
import Keys._
import com.typesafe.sbt.packager.linux.{LinuxPackageMapping, LinuxSymlink}
import com.typesafe.sbt.packager.rpm.RpmDependencies

import com.typesafe.sbt.packager.Keys._
import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._
import com.typesafe.sbt.packager.archetypes.TemplateWriter

import com.typesafe.sbt.packager.universal.Keys.stagingDirectory

import play.Play.autoImport._
import PlayKeys._

import com.typesafe.sbt.web._
import com.typesafe.sbt.web.SbtWeb._
import com.typesafe.sbt.web.Import._
import com.typesafe.sbt.web.Import.WebKeys._



object ApplicationBuild extends Build {

  val appVersion = "2.5.5"

  val hiveVersion = "0.13.1"

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

  val willrogers = Project(
      "willrogers",
      file("apps/willrogers")
  ).enablePlugins(play.PlayScala, SbtWeb)
   .settings( version := appVersion,

     ////sbt-web doesn't automatically include the assets 

     (unmanagedResourceDirectories in Compile) += (webTarget in Assets).value,

     (packageBin in Compile) <<= (packageBin in Compile).dependsOn(assets in Assets),

     (packageBin in Rpm) <<= (packageBin in Rpm).dependsOn(packageBin in Compile)

   ).settings( AppSettings: _* ).settings(RpmSettings: _* ).dependsOn(core, engine, hadoop )

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

      organization := "com.tagged.satisfaction",

      version := appVersion,

      packageSummary := "wyman",

      libraryDependencies ++= testDependencies,

      publishMavenStyle := true,

      publishTo := Some("subversion-releases" at "http://artifactory.tagged.com:8081/artifactory/libs-release-local"),

      credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
 
      isSnapshot := true
  ) 

  def AppSettings =  CommonSettings ++ Seq(
     javacOptions in Compile ++= Seq("-source", "1.7", "-target", "1.7"),

     unmanagedResourceDirectories in Assets += baseDirectory.value / "public"

    
     ///(managedClasspath in Runtime) += (packageBin in Assets).value


  )


  def resolveRpmVersion() = {
    sys.env.get("BUILD_NUMBER") match {
      case Some(buildNumber) => buildNumber
      case None => appVersion
    }
  }

  def RpmSettings = packagerSettings ++ deploymentSettings ++ packageArchetype.java_server ++  Seq(
    maintainer in Linux := "Jerome Banks jbanks@tagged.com",
    packageSummary in Linux := "Satisfaction",
    packageDescription in Linux := "The Next Generation Hadoop Scheduler",
    daemonUser in Linux := "satisfaction",
    daemonGroup in Linux := "satisfaction",
    normalizedName in Linux := "satisfaction",


    linuxPackageMappings <++= (mappings in Universal) map { universalDir => 
        universalDir.filter( {  _._2.toString.startsWith("/usr/share") } ).map { packageMapping( _ ) } 
    },


    mappings in Universal <+= (packageBin in Compile, baseDirectory ) map { (_, base) =>
       val conf = base / "conf" / "application.conf"
       conf -> "conf/application.conf"
    },

    mappings in Universal <+= (packageBin in Compile, baseDirectory ) map { (_, base) =>
       val conf = base / "conf" / "logger.xml"
       conf -> "conf/logger.xml"
    },


    name in Rpm := "satisfaction-scheduler",
    ///version in Rpm := appVersion,
    version in Rpm := resolveRpmVersion(),

    ////rpmRelease in Rpm:= resolveRpmVersion(),
    rpmRelease in Rpm := "1",
    rpmBrpJavaRepackJars := true,
    packageSummary in Rpm := "wyman",
    packageSummary in Linux := "wyman",
    rpmVendor in Rpm := "Tagged.com",
    rpmUrl in Rpm:= Some("http:/github.com/tagged/satisfaction"),
    rpmLicense in Rpm:= Some("Apache License Version 2"),
    packageDescription in Rpm := "Next Generation Hadoop Scheduler",
    rpmGroup in Rpm:= Some("satisfaction"),

    rpmPreun := Option("""
if [[ $1 == 0 ]] ; then
    echo "Shutdown willrogers"
    service willrogers stop || echo "Could not stop willrogers"
fi"""),

    rpmPost := Option("""
export JAVA_HOME=/usr/java/default
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_HOME=/usr/lib/hadoop

""")


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
  ).excluding("commons-daemon", "commons-daemon" )
	.excluding("junit","junit")
	.excluding("log4j", "log4j")
        .excluding("org.slf4j","slf4j-log4j12")
        .excludingGroup("org.jboss.netty" ) ++ testDependencies ++ metastoreDependencies

  def coreDependencies = Seq(
    ("org.slf4j" % "slf4j-api" % "1.7.7"),
    ("com.github.nscala-time" %% "nscala-time" % "1.2.0"),
    ("org.scala-lang" % "scala-library" % "2.10.2" ),
    ("org.apache.commons" % "commons-email" % "1.3.3" )
  ) ++ testDependencies 

  def jsonDependencies = Seq(
   ("org.json4s" %% "json4s-jackson" % "3.2.9" )
 )

  def metastoreDependencies = Seq(
	  ("org.apache.hive" % "hive-common" % hiveVersion),
	  ("org.apache.hive" % "hive-shims" % hiveVersion),
	  ("org.apache.hive" % "hive-metastore" % hiveVersion),
	  ("org.apache.hive" % "hive-serde" % hiveVersion),
	  ("org.apache.hive" % "hive-exec" % hiveVersion),
	  ("org.apache.thrift" % "libfb303" % "0.7.0"),
	  ("com.tagged.analytics" % "avro-serde" % "0.13.1-jdb")
  ).excluding( "log4j", "log4j" ).excluding("org.slf4j", "slf4j-log4j12")
   .excluding("org.jboss.netty", "netty")

  def hiveDependencies = Seq(
	  ("org.apache.hive" % "hive-common" % hiveVersion),
	  ("org.apache.hive" % "hive-exec" % hiveVersion),
	  ("org.apache.hive" % "hive-metastore" % hiveVersion),
	  ("org.apache.hive" % "hive-service" % hiveVersion),
	  ("org.apache.hive" % "hive-serde" % hiveVersion),
	  ("org.apache.hive" % "hive-shims" %   hiveVersion ),
	  ("org.apache.hive" % "hive-hbase-handler" % hiveVersion),
	  ("org.apache.hive" % "hive-jdbc" % hiveVersion),
	  ("org.apache.hive" % "hive-service" % hiveVersion ),
	  ("org.apache.thrift" % "libfb303" % "0.7.0" ),
	  ("org.antlr" % "antlr-runtime" % "3.4" ),
	  ("org.antlr" % "antlr" % "3.0.1" )
  ).excluding("org.slf4j", "slf4j-log4j12")
   .excluding("org.jboss.netty", "netty") 
   .excludingGroup("org.jboss.netty")  ++ metastoreDependencies ++ testDependencies


  def engineDependencies = Seq(
    ("com.typesafe.akka" %% "akka-actor" % "2.3.9"),
    ("org.quartz-scheduler" % "quartz" % "2.2.1"),
    ("ch.qos.logback" % "logback-classic" % "1.0.13" ),
    ("com.typesafe.slick" %% "slick" % "2.0.2"),
    ("com.h2database" % "h2" % "1.3.170"),
    ("com.typesafe.slick" %% "slick" % "2.0.2"),
    ("nl.grons" %% "metrics-scala" % "3.3.0_a2.2"),
    ("ch.qos.logback" % "logback-classic" % "1.0.13" )
  ) ++ testDependencies ++ jsonDependencies



  def Resolvers = resolvers ++= Seq(
      "artifactory.tagged.com" at "https://artifactory.tagged.com/artifactory/repo",
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Maven Central" at "http://repo1.maven.org/maven2",
      "Apache Maven Repository" at "http://people.apache.org/repo/m2-snapshot-repository/",
      "ScalaToolsMaven2Repository" at "http://scala-tools.org/repo-releases",
      "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
  )

}



