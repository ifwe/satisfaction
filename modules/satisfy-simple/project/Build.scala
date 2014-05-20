import sbt._
import Keys._




object SatisfySimpleBuild extends Build {

  val main = Project(
      "satisfy-simple", file(".")
  ).settings(LibrarySettings: _*).settings(
      version := "2.6"
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
   ("com.klout.satisfaction" %% "satisfaction-core" % "2.0.1"),
    "org.specs2" %% "specs2" % "1.14" % "test"
   )


   def commonResolvers = Seq(
     "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
     "releases"  at "http://oss.sonatype.org/content/repositories/releases",
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
