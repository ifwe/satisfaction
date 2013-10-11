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
import com.klout.satisfaction.executor.actors.GoalState
import java.io.FileInputStream
import java.io.File
import models.PlumbGraph
import models.VariableFormHandler
import collection._
import play.mvc.Results

object SatisfyGoalPage extends Controller {

    def satisfyGoalAction(projName: String, goalName: String) = Action { implicit request =>
        println(s" Satisfying Goal  $projName $goalName")
        //// Can't use standard Play Form object ...
        ////  because  witness can have different variables
        //// XXX For now assume everything is "dt", and "network_abbr"
        println(request.body.asFormUrlEncoded)
        val goalOpt = getGoalByName(projName, goalName)
        goalOpt match {
            case Some(goal) =>
                val variableFormHandler = new VariableFormHandler(goal.variables)
                variableFormHandler.processRequest(request) match {
                    case Right(subst) =>
                        val witness = Witness(subst)
                        val status: GoalStatus = this.satisfyGoal(goal, witness)
                        println(" Got Goal Stqtus = " + status.state)

                        Redirect(s"/goalstatus/$projName/$goalName")
                    case Left(errorMessages) =>
                        /// Bring him back to the first page
                        val pg = ProjectPage.getFullPlumbGraphForGoal(goal)
                        Ok(views.html.satisfygoal(projName, goalName, errorMessages.toList, goal.variables.toList, Some(pg)))

                }
            case None =>
                NotFound(s"Dude, we can't find the goal ${goalName} in poject ${projName}")
        }
    }

    def showSatisfyForm(projName: String, goalName: String) = Action {
        val goal = getGoalByName(projName, goalName)
        goal match {
            case Some(_) =>
                ///val pg = ProjectPage.getPlumbGraphForGoal(goal.get)
                val pg = ProjectPage.getFullPlumbGraphForGoal(goal.get)
                Ok(views.html.satisfygoal(projName, goalName, List(), goal.get.variables.toList, Some(pg)))
            case None =>
                NotFound(s"Dude, we can't find the goal ${goalName} in poject ${projName}")
        }
    }

    /// XXXX TODO .. get status for all running witnesses
    def goalStatus(projName: String, goalName: String) = Action {
        getStatusForGoal(projName, goalName) match {
            case Some(status) =>
                val plumb = plumbGraphForStatus(status)
                if (status.state == GoalState.SatifyingSelf) {
                    /// Read log files 
                    val logs = readLogFile(status.goal, status.witness)
                    Ok(views.html.goalstatus(projName, goalName, status, Some(logs), Some(plumb)))
                } else {
                    Ok(views.html.goalstatus(projName, goalName, status, None, Some(plumb)))
                }
            case None =>
                NotFound(s"Dude, we can't find the goal ${goalName} in poject ${projName}")
        }
    }

    /// XXX  pass in method to for customize ??
    def plumbGraphForStatus(status: GoalStatus): PlumbGraph = {
        val pg = new PlumbGraph
        pg.divWidth = 2000
        pg.divHeight = 500
        val nodeMap: mutable.Map[String, PlumbGraph.NodeDiv] = mutable.Map()

        val topNodeDiv = new PlumbGraph.NodeDiv(
            divContent = status.goal.name
        )
        topNodeDiv.width = 10
        topNodeDiv.height = 10
        topNodeDiv.posX = 25
        topNodeDiv.posY = 2
        topNodeDiv.color = getColorForState(status.state)
        pg.addNodeDiv(topNodeDiv)

        plumbGraphForStatusRecursive(pg, status, topNodeDiv, nodeMap)

        pg
    }

    def getColorForState(state: GoalState.Value): String = {
        state match {
            case GoalState.Aborted               => "Gray"
            case GoalState.Success               => "Green"
            case GoalState.AlreadySatisfied      => "DarkGreen"
            case GoalState.Failed                => "Red"
            case GoalState.DepFailed             => "Orange"
            case GoalState.SatifyingSelf         => "Purple"
            case GoalState.WaitingOnDependencies => "Yellow"
            case _                               => "White"
        }

    }
    /**
     *  Do recursive layout of dependencies
     */
    def plumbGraphForStatusRecursive(pg: PlumbGraph, currentGoal: GoalStatus, currentNode: PlumbGraph.NodeDiv, nodeMap: mutable.Map[String, PlumbGraph.NodeDiv]): Unit = {
        val numDeps = currentGoal.dependencyStatus.size
        var startX = currentNode.posX - (currentNode.width + 2) * numDeps / 2
        currentGoal.dependencyStatus.values.foreach { depGoalStatus =>
            var depNode: PlumbGraph.NodeDiv = null
            if (nodeMap.contains(depGoalStatus.goal.name))
                depNode = nodeMap.get(depGoalStatus.goal.name).get
            else {
                depNode = new PlumbGraph.NodeDiv(divContent = depGoalStatus.goal.name)
                nodeMap.put(depGoalStatus.goal.name, depNode)
                pg.addNodeDiv(depNode)
            }

            depNode.width = currentNode.width
            depNode.height = currentNode.height
            depNode.posY = currentNode.posY + currentNode.height + 3
            depNode.posX = startX + depNode.width + 3
            depNode.color = getColorForState(depGoalStatus.state)
            startX = depNode.posX
            pg.addConnection(depNode, currentNode)

            plumbGraphForStatusRecursive(pg, depGoalStatus, depNode, nodeMap)
        }

    }

    def readLogFile(goal: Goal, witness: Witness): String = {
        //// XXX TODO
        //// Come up with reasonable naming convention for log files ...

        val file = new File(witness.substitution.raw.mkString("_").replace(" ", "_").replace("->", "_"))
        if (file.exists()) {
            io.Source.fromFile(file).getLines.mkString("<br>\n")
        } else {
            ""
        }
    }

    def getStatusForGoal(projName: String, goalName: String): Option[GoalStatus] = {

        /// XXX TODO Push to ProofEngine ..
        //// use project name 
        val statList = ProofEngine.getGoalsInProgress.filter(_.goal.name.equals(goalName))
        if (statList.size > 0)
            Some(statList.head)
        else
            None
    }

    def getGoalByName(projName: String, goalName: String): Option[Goal] = {

        val project: Track = getProjectByName(projName)
        println("Size of project allGoals is " + project.allGoals.size)
        println(project.allGoals)
        project.allGoals.find(_.name.equals(goalName.trim))
    }

    def satisfyGoal(goal: Goal, witness: Witness): GoalStatus = {

        //// instead of holding onto the Future, 
        //// just ask again to get current status,
        //// and bring up project status page
        ProofEngine.satisfyGoal(goal, witness)
        ProofEngine.getStatus(goal, witness)
    }

    def getProjectByName(projName: String): Track = ProjectPage.getSimpleProject

}