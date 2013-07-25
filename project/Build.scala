import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appVersion = "0.1"

  val common = Project(
      "common",
      file("modules/common")
  ).settings(CommonSettings: _*).settings(version := appVersion)

  val samples = Project(
      "samples",
      file("modules/samples")
  ).settings(CommonSettings: _*).settings(version := appVersion).dependsOn(common)

  val api = play.Project(
      "api",
      appVersion,
      path = file("apps/api")
  ).settings(CommonSettings: _*).dependsOn(common)

  def CommonSettings = Dependencies ++ ScalariformSettings ++ Resolvers ++ Seq(
      scalacOptions ++= Seq(
          "-unchecked",
          "-feature",
          "-deprecation",
          "-language:existentials",
          "-language:postfixOps",
          "-language:implicitConversions"
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
      "com.klout" %% "scoozie" % "0.3.7"
  )

  def Resolvers = resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Klout Maven libs Repository" at "http://maven-repo:8081/artifactory/libs-release",
      "Klout Remote Repositories" at "http://maven-repo:8081/artifactory/remote-repos",
      "Klout Maven external libs Repository" at "http://maven-repo:8081/artifactory/ext-release-local",
      "Klout Maven external snapshots Repository" at "http://maven-repo:8081/artifactory/ext-snapshot-local",
      "local-maven-repo-releases" at "http://maven-repo:8081/artifactory/libs-release-local",
      "local-maven-repo-snapshots" at "http://maven-repo:8081/artifactory/libs-snapshot-local/"
  )


}
