package com.klout.satisfaction
package api
package actors

import common.dsl._

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

import scala.concurrent.duration._

import play.api.libs.concurrent.Execution.Implicits._

object `package` {

    implicit val timeout = Timeout(5.seconds)

    val system = ActorSystem("projects")
}

class ProjectManager extends Actor {

    var currentProjects: Map[String, ActorRef] = Map.empty

    lazy val fs = {
        val conf = new Configuration()
        conf.set("fs.default.name", "jobs-aa-hnn:8020")
        val fs = FileSystem.get(conf)
        fs
    }

    def receive = {
        case AddProject(path, name) =>
            val project = loadProjectFromJar(path, name)
            currentProjects += name -> context.actorOf(Props(new ProjectOwner(project)))

        case RemoveProject(name) =>
            currentProjects -= name

        case GetProject(name: String) =>
            (currentProjects get name) match {
                case Some(owner) =>
                    (owner ? YourProject).mapTo[MyProject] map (result => ProjectResult(Some(result.project))) pipeTo sender
                case _ =>
                    sender ! ProjectResult(None)
            }

        case GetProjects =>
            sender ! ProjectList(currentProjects.keys.toSet)
    }

    def loadProjectFromJar(path: String, name: String): Project = {
        // First copy the file to local tmp directory.
        val sourcePath = new Path(path)
        val jarName = sourcePath.getName
        val randomSuffix = java.util.UUID.randomUUID().toString
        val destPath = new Path(s"/tmp/${jarName}_${randomSuffix}")
        fs.copyToLocalFile(sourcePath, destPath)

        // Load the class from the jar.
        import java.net._
        val url = new URL(s"file://${destPath.toString}")
        val urls = Array(url)
        val ucl = new URLClassLoader(urls, classOf[ProjectProvider].getClassLoader)
        val clazz = ucl.loadClass(name)
        fs.delete(destPath, false)

        // Get an instance of the project. 
        val provider = clazz.getField("MODULE$").get(clazz).asInstanceOf[ProjectProvider]
        val project = provider.project

        project
    }
}

class ProjectOwner(project: Project) extends Actor {

    def receive = {
        case YourProject =>
            sender ! MyProject(project)
    }

}

case object YourProject
case class MyProject(project: Project)

case class AddProject(jarPath: String, name: String)
case class RemoveProject(name: String)
case class GetProject(name: String)
case object GetProjects

case class ProjectResult(project: Option[Project])
case class ProjectList(names: Set[String])

