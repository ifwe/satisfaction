package controllers

import play.api._
import play.api.mvc._
import Results._

import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.duration._

import com.klout.satisfaction.executor.api._

object Projects extends Controller {

    def addProject(jarPath: String, projectName: String) = Action {
        Api.addProject(jarPath, projectName)
        Ok("project added")
    }

    def removeProject(projectName: String) = Action {
        Api.removeProject(projectName)
        Ok("project removed")
    }

    def getProjects() = Action {
        AsyncResult {
            Api.getProjects() map { projects =>
                Ok("projects: " + projects)
            }
        }
    }

    def getProject(name: String) = Action {
        AsyncResult {
            Api.getProject(name) map { project =>
                Ok("project: " + project)
            }
        }
    }

    def main(argv: Array[String]): Unit = {
        val allProjects = SyncApi.getProjects

        println(" Number of projects = " + allProjects.names.size)
        for (pname <- allProjects.names) {
            val proj = SyncApi.getProject(pname)
            println("project is " + proj.project)
        }

    }

}
