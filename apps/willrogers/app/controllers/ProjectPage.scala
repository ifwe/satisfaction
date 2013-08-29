package controllers

import play.api._
import play.api.mvc._
import Results._
import play.api.libs.concurrent.Execution.Implicits._
import scala.concurrent.duration._
import com.klout.satisfaction.executor.api._
import com.klout.satisfaction.Project

object ProjectPage extends Controller {

    def showProject(projName: String) = Action {
        ///val project = SyncApi.getProject(projName)
        val project = getSimpleProject
        //val internalGoals = project.project.get.internalGoals.toList
        //val externalGoals = project.project.get.externalGoals.toList
        ///val internalGoals = project.topLevelGoals.toList
        val internalGoals = project.allGoals.toList
        val externalGoals = project.externalGoals.toList

        Ok(views.html.showproject(projName, internalGoals map (_.name), externalGoals map (_.name)))

    }

    def getSimpleProject(): Project = {
        com.klout.satisfaction.projects.MaxwellProject.Project
    }

}
