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

    def satisfyGoalAction(trackName: String, goalName: String) = Action { implicit request =>
        println(s" Satisfying Goal  $trackName $goalName")
        //// Can't use standard Play Form object ...
        ////  because  witness can have different variables
        //// XXX For now assume everything is "dt", and "network_abbr"
        println(request.body.asFormUrlEncoded)
        val goalOpt = getTrackGoalByName(trackName, goalName)
        goalOpt match {
            case Some(goalTuple) =>
                val variableFormHandler = new VariableFormHandler(goalTuple._2.variables)
                variableFormHandler.processRequest(request) match {
                    case Right(subst) =>
                        val witness = Witness(subst)
                        val status: GoalStatus = this.satisfyGoal(goalTuple._1, goalTuple._2, witness)
                        println(" Got Goal Stqtus = " + status.state)

                        Redirect(s"/goalstatus/$trackName/$goalName")
                    case Left(errorMessages) =>
                        /// Bring him back to the first page
                        val pg = ProjectPage.getFullPlumbGraphForGoal(goalTuple._2)
                        Ok(views.html.satisfygoal(trackName, goalName, errorMessages.toList, goalTuple._2.variables.toList, Some(pg)))

                }
            case None =>
                NotFound(s"Dude, we can't find the goal ${goalName} in the Track ${trackName}")
        }
    }
    
    
    def currentStatusAction() = Action {
        //// XXX Apply sort order to statuses
        val statList = ProofEngine.getGoalsInProgress
        println(" Number of Goals in Progress are " + statList.size)
        Ok(views.html.currentstatus( statList))
    }

    def showSatisfyForm(trackName: String, goalName: String) = Action {
        val goal = getTrackGoalByName(trackName, goalName)
        goal match {
            case Some(tuple) =>
                ///val pg = ProjectPage.getPlumbGraphForGoal(goal.get)
                val pg = ProjectPage.getFullPlumbGraphForGoal(tuple._2)
                Ok(views.html.satisfygoal(trackName, goalName, List(), tuple._2.variables.toList, Some(pg)))
            case None =>
                NotFound(s"Dude, we can't find the goal ${goalName} in Track ${trackName}")
        }
    }

    def goalStatus(trackName: String, goalName: String) = Action {
        println(s" GOAL STATUS $trackName :: $goalName ")
        getStatusForGoal(trackName, goalName) match {
            case Some(status) =>
                val plumb = plumbGraphForStatus(status)
                if (status.state == GoalState.Running) {
                    /// Read log files 
                    println(s" Running Job for status $status")
                    val logs = readLogFile(status.track, status.goalName, status.witness)
                    Ok(views.html.goalstatus(trackName, goalName, status, Some(logs), Some(plumb)))
                } else {
                    Ok(views.html.goalstatus(trackName, goalName, status, None, Some(plumb)))
                }
            case None =>
              println( " NO STATUS FOUND -- DISPlaying history ")
              goalHistory( trackName, goalName)
                ///NotFound(s"Dude, we can't find the goal ${goalName} in Track ${projName}")
        }
    }
    
    
    def goalHistory( trackName : String, goalName : String ) =  {
         val goalPaths = LogWrapper.getLogPathsForGoal( trackName, goalName).toList
         Ok( views.html.goalhistory( trackName, goalName, goalPaths) )
      
    }

    /// XXX  pass in method to for customize ??
    def plumbGraphForStatus(status: GoalStatus): PlumbGraph = {
        val pg = new PlumbGraph
        pg.divWidth = 2000
        pg.divHeight = 500
        val nodeMap: mutable.Map[String, PlumbGraph.NodeDiv] = mutable.Map()

        val topNodeDiv = new PlumbGraph.NodeDiv(
            divContent = status.goalName
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
            case GoalState.DependencyFailed             => "Orange"
            case GoalState.Running         => "Purple"
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
            if (nodeMap.contains(depGoalStatus.goalName))
                depNode = nodeMap.get(depGoalStatus.goalName).get
            else {
                depNode = new PlumbGraph.NodeDiv(divContent = depGoalStatus.goalName)
                nodeMap.put(depGoalStatus.goalName, depNode)
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

    def readLogFile(track : TrackDescriptor, goalName: String, witness: Witness): String = {
        val logFile = LogWrapper.logPathForGoalWitness(track, goalName, witness)

        if (logFile.exists()) {
            io.Source.fromFile(logFile).getLines.mkString("<br>\n")
        } else {
            ""
        }
    }

    def getStatusForGoal(trackName: String, goalName: String): Option[GoalStatus] = {

        /// XXX TODO Push to ProofEngine ..
        //// use project name 
         ///val allStats = ProofEngine.getGoalsInProgress 
         ///allStats.foreach { stat => println( stat.track.name  + " :: " + stat.goal.name)}
        val statList = ProofEngine.getGoalsInProgress.filter(_.track.trackName.equals(trackName)).filter(_.goalName.equals(goalName))
        if (statList.size > 0)
            Some(statList.head)
        else
            None
    }
    
    

    def getTrackGoalByName(trackName: String, goalName: String): Option[Tuple2[Track,Goal]] = {

        val track: Track = getTrackByName(trackName)
        ///println("Size of project allGoals is " + track.allGoals.size)
        println(track.allGoals)
        val someGoal = track.allGoals.find(_.name.trim.equals(goalName.trim))
        someGoal match {
          case Some(goal) =>
            Some(Tuple2[Track,Goal]( track, goal))
          case None => None
        }
    }

    def satisfyGoal(track : Track, goal: Goal, witness: Witness): GoalStatus = {

        //// instead of holding onto the Future, 
        //// just ask again to get current status,
        //// and bring up project status page
        ProofEngine.satisfyGoal(track, goal, witness)
        ProofEngine.getStatus(track, goal, witness)
    }

    def getTrackByName(trackName: String): Track = {
        /// XXX Figure out how to use version and variant
        val trackDesc = TrackDescriptor( trackName)
        val trackOpt : Option[Track] = ProjectPage.trackFactory.getTrack( trackDesc)
        trackOpt.get
    } 

}