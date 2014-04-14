import sbt._
import Keys._




object SatisfySimpleBuild extends Build {

  val main = Project(
      "satisfy-simple", file(".")
  ).settings(LibrarySettings: _*).settings(
      version := "2.4"
  )

  def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) =
      items.map(_.exclude(group, artifact))

  implicit def dependencyFilterer(deps: Seq[ModuleID]) = new Object {
     def excluding(group: String, artifactId: String) =
        deps.map(_.exclude(group, artifactId))

     def excludingGroup(group: String) =
        deps.map(_.exclude(group, "*"))
  }


  def commonDependencies = Seq(
   ("com.klout.satisfaction" %% "satisfaction-core" % "0.3.1"),
    "org.specs2" %% "specs2" % "1.14" % "test"
   )


   def commonResolvers = Seq(
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



   def BaseSettings = Seq(
      resolvers ++= commonResolvers,

      organization := "com.klout.satisfy",

      scalaVersion := "2.10.2",

	  startYear := Some(2013),

      scalacOptions ++= Seq(
	     "-deprecation",
	     "-unchecked",
	     "-feature",
	     "-language:existentials",
	     "-language:postfixOps",
	     "-language:implicitConversions"),

         logLevel in compile := Level.Info,

         libraryDependencies ++= commonDependencies,

         publishTo := Some("kloutLibraryReleases" at "http://maven-repo:8081/artifactory/libs-release-local"),

         credentials := Credentials(Path.userHome / ".ivy2" / ".credentials") :: Nil,

         publishTo := Some("kloutLibraryReleases" at "http://maven-repo:8081/artifactory/libs-release-local"),

         credentials := Credentials(Path.userHome / ".ivy2" / ".credentials") :: Nil,

		 exportJars := false
		  

   )  


/**
  def uploadSettings = {

     Seq (
	   hdfsURI := new java.net.URI("hdfs://jobs-dev-hnn"),
	   baseHdfsPath := "/user/satisfaction/",
	   pathPattern := "/track/${project}/version_2.1/",
       hiveDependencies := Seq(
	      ("com.google.guava" % "guava" % "11.0.1"),
		  ("joda-time" % "joda" % "1.6"),
		  ("com.klout" % "brickhouse" % "0.5.5")
	   )

     ) ++ UploadPlugin.UploadTasks
   }
   **/

   def LibrarySettings = BaseSettings
}
