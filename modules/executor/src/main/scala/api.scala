package com.klout.satisfaction
package executor
package api

import actors._

import akka.actor._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._

import scala.concurrent._

import org.apache.hadoop.fs._

object Api {

    ///lazy val projectManager = system.actorOf(Props[ProjectManager])
    lazy val projectManager = system.actorFor(" Bogus")

    def addProject(jarPath: String, projectName: String) {
        projectManager ! AddProject(jarPath, projectName)
    }

    def removeProject(projectName: String) {
        projectManager ! RemoveProject(projectName)
    }

    def getProjects(): Future[ProjectList] = {
        (projectManager ? GetProjects).mapTo[ProjectList]
    }

    def getProject(name: String): Future[ProjectResult] = {
        (projectManager ? GetProject(name)).mapTo[ProjectResult]
    }

    def initProjects(hdfsRootPath: String) {
        val rootPath = new Path(hdfsRootPath)
        val fileStatuses = fs.listStatus(rootPath)
        fileStatuses foreach { status =>
            val path = status.getPath
            val name = path.getName
            val jarPath = fs.listStatus(path)(0).getPath.toString
            addProject(jarPath, name)
        }
    }
}

object SyncApi {

    def getProjects(): ProjectList = {
        Await.result(Api.getProjects(), 1 minute)
    }

    def getProject(name: String): ProjectResult = {
        Await.result(Api.getProject(name), 1 minute)
    }
}