package controllers

import play.api._
import play.api.mvc._
import Results._

import play.api.libs.concurrent.Execution.Implicits._

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

import scala.concurrent.duration._

import com.klout.satisfaction.executor.actors._

object Projects extends Controller {

    val projectManager = system.actorOf(Props[ProjectManager])

    def addProject(jarPath: String, projectName: String) = Action {
        projectManager ! AddProject(jarPath, projectName)
        Ok("project added")
    }

    def removeProject(projectName: String) = Action {
        projectManager ! RemoveProject(projectName)
        Ok("project removed")
    }

    def getProjects() = Action {
        AsyncResult {
            (projectManager ? GetProjects).mapTo[ProjectList] map { projects =>
                Ok("projects: " + projects)
            }
        }
    }

    def getProject(name: String) = Action {
        AsyncResult {
            (projectManager ? GetProject(name)).mapTo[ProjectResult] map { project =>
                Ok("project: " + project)
            }
        }
    }

}
