package com.klout.satisfaction
package api

import common.dsl._

import akka.actor._

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

class ProjectManager extends Actor {

    var currentProjects: Map[String, Project] = Map.empty

    lazy val fs = {
        val conf = new Configuration()
        conf.set("fs.default.name", "jobs-aa-hnn:8020")
        val fs = FileSystem.get(conf)
        fs
    }

    def receive = {
        case AddProject(path, name) =>
            val project = loadProjectFromJar(path, name)
            currentProjects += name -> project

        case RemoveProject(name) =>
            currentProjects -= name

        case GetProject(name: String) =>
            sender ! ProjectResult(currentProjects get name)

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

case class AddProject(jarPath: String, name: String)
case class RemoveProject(name: String)
case class GetProject(name: String)
case object GetProjects

case class ProjectResult(project: Option[Project])
case class ProjectList(names: Set[String])

