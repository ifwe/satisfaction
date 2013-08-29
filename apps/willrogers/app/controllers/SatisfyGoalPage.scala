package controllers

import play.api._
import play.api.data._
import play.api.data.Forms._
import play.mvc.Controller
import play.api.mvc.Results._
import play.api.data.validation.Constraints._
import play.api.mvc.Action
import models.VariableHolder
import hive.ms._
import play.api.mvc.Call
import com.klout.satisfaction._
import com.klout.satisfaction.executor._
import com.klout.satisfaction.executor.actors.ProofEngine
import com.klout.satisfaction.executor.actors._
import java.io.FileInputStream
import java.io.File

object SatisfyGoalPage extends Controller {
    //// need to figure out how to handle 
    //// dynamic form fields
    ///// Probably can't use Play Forms directly
    val variablesForm = Form[VariableHolder](
        mapping("dt" -> nonEmptyText,
            "network_abbr" -> text,
            "service_id" -> text
        ) (VariableHolder.apply)(VariableHolder.unapply))

    def satisfyGoalAction(projName: String, goalName: String) = Action { implicit request =>
        println(s" Satisfying Goal  $projName $goalName")
        //// Can't use standard Play Form object ...
        ////  because  witness can have different variables
        //// XXX For now assume everything is "dt", and "network_abbr"
        variablesForm.bindFromRequest.fold(
            formWithErrors => Status(500),
            variableHolder => {

                println(" XXX satisfying goal  " + goalName + " with variable  " + variableHolder)
                val goalOpt = getGoalByName(projName, goalName)
                goalOpt match {
                    case Some(goal) =>
                        val witness = Witness(Substitution(VariableAssignment("dt", variableHolder.dt),
                            VariableAssignment("service_id", variableHolder.service_id.toInt)))

                        val status: GoalStatus = this.satisfyGoal(goal, witness)

                        println(" Got Goal Stqtus = " + status.state)
                        ///Redirect(views.html.goalstatus(projName, goalName, status, None))
                        Redirect(s"/goalstatus/$projName/$goalName")

                    case None =>
                        /// XXX TODO add errMess lune
                        Ok(views.html.satisfygoal(projName, goalName, List.empty))
                }
            }
        )
    }

    def showSatisfyForm(projName: String, goalName: String) = Action {
        val goal = getGoalByName(projName, goalName)
        goal match {
            case Some(_) =>
                Ok(views.html.satisfygoal(projName, goalName, goal.get.variables.toList))
            case None =>
                /// XXX TODO error page 
                Ok(views.html.satisfygoal(projName, goalName, List.empty))
        }
    }

    /// XXXX TODO .. get status for all running witnesses
    def goalStatus(projName: String, goalName: String) = Action {
        val goal = getGoalByName(projName, goalName)
        goal match {
            case Some(_) =>
                val status = getStatusForGoal(projName, goalName).toSeq.head
                if (status.state == GoalState.SatifyingSelf) {
                    /// Read log files 
                    val logs = readLogFile(status.goal, status.witness)
                    Ok(views.html.goalstatus(projName, goalName, status, Some(logs)))
                } else {
                    Ok(views.html.goalstatus(projName, goalName, status, None))
                }
            case None =>
                /// XXX TODO error page 
                Ok(views.html.satisfygoal(projName, goalName, List.empty))
        }
    }

    def readLogFile(goal: Goal, witness: Witness): String = {
        //// XXX TODO
        //// Come up with reasonable naming convention for log files ...
        val lines: String = io.Source.fromFile(new File(witness.substitution.raw.mkString("_").replace(" ", "_").replace("->", "_"))).getLines.mkString("\n")

        lines

    }

    def getStatusForGoal(projName: String, goalName: String): Set[GoalStatus] = {

        ProofEngine.getGoalsInProgress.filter(_.goal.name.equals(goalName))

    }

    def getGoalByName(projName: String, goalName: String): Option[Goal] = {

        val project: Project = getProjectByName(projName)
        project.allGoals.find(_.name.equals(goalName.trim))
    }

    def satisfyGoal(goal: Goal, witness: Witness): GoalStatus = {

        //// instead of holding onto the Future, 
        //// just ask again to get current status,
        //// and bring up project status page
        ProofEngine.satisfyGoal(goal, witness)
        ProofEngine.getStatus(goal, witness)
    }

    def getProjectByName(projName: String): Project = ProjectPage.getSimpleProject

}