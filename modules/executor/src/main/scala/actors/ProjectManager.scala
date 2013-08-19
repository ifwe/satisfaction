package com.klout.satisfaction
package executor
package actors

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits._

class ProjectManager extends Actor with ActorLogging {

    var currentProjects: Map[String, ActorRef] = Map.empty

    def receive = {
        case AddProject(path, name) =>
            val project = loadProjectFromJar(path, name)
            val projectActor = context.actorOf(Props(new ProjectOwner(project)), name)
            currentProjects += name -> projectActor

        case RemoveProject(name) =>
            val removedProject = currentProjects get name
            removedProject foreach { projectActor =>
                context.stop(projectActor)
            }
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
        val clazzName = if (name endsWith "$") name else (name + "$")
        val clazz = ucl.loadClass(clazzName)
        fs.delete(destPath, false)

        // Get an instance of the project. 
        val provider = clazz.getField("MODULE$").get(clazz).asInstanceOf[ProjectProvider]
        val project = provider.project

        project
    }
}