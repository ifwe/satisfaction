package willrogers
package controllers

import play.api._
import java.net.URLDecoder
import java.net.URLDecoder._
import play.api.data._
import play.api.data.Forms._
import play.mvc.Controller
import play.api.mvc.Results._
import play.api.data.validation.Constraints._
import play.api.mvc.Action
import models.VariableHolder
import play.api.mvc.Call
import satisfaction._
import satisfaction.engine._
import satisfaction.engine.actors._
import java.io.FileInputStream
import java.io.File
import models.PlumbGraph
import models.VariableFormHandler
import collection._
import play.mvc.Results
import models.HtmlUtil
import fs.LocalFileSystem
import fs._

object SatisfyGoalPage extends Controller with Logging {
    val proofEngine : ProofEngine = Global.proofEngine

    def satisfyGoalAction(trackName: String, goalName: String) = Action { implicit request =>
        info(s" Satisfying Goal  $trackName $goalName")
        //// Can't use standard Play Form object ...
        ////  because  witness can have different variables
        val goalOpt = getTrackGoalByName(trackName, goalName)
        goalOpt match {
            case Some(goalTuple) =>
                val variableFormHandler = new VariableFormHandler(goalTuple._2.variables)
                variableFormHandler.processRequest(request) match {
                    case Right(witness) =>
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
        val statList = proofEngine.getGoalsInProgress
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
                    println(s" Running Job for status $status")
                 Ok(views.html.goalstatus(trackName, goalName, status.witness, status, Some(plumb)))
            case None =>
              println( " NO STATUS FOUND -- DISPlaying history ")
              goalHistory( trackName, goalName)
                ///NotFound(s"Dude, we can't find the goal ${goalName} in Track ${projName}")
        }
    }
    
    
    def goalHistory( trackNameNC : String, goalNameNC : String ) =  {
        val trackName = trackNameNC.replace('+', ' ')
        val goalName = goalNameNC.replace('+', ' ')

         val goalPaths = LogWrapper.getLogPathsForGoal( trackName, goalName).map( _.path.toString ).toList
         Ok( views.html.goalhistory( trackName, goalName, goalPaths) )
      
    }

    /// XXX  pass in method to for customize ??
    def plumbGraphForStatus(status: GoalStatus): PlumbGraph = {
        val pg = new PlumbGraph
        pg.divWidth = 2000
        pg.divHeight = 500
        val nodeMap: mutable.Map[String, PlumbGraph.NodeDiv] = mutable.Map()
        
        val progTracker = status.progressCounter
        val pctStr = progTracker match {
          case Some(progress) => {
            f" ${progress.progressPercent*100%2.2f} %%"
          }
          case None => { "" }
        }

        val topNodeDiv = new PlumbGraph.NodeDiv(
            divContent = status.goalName + pctStr
        )
        topNodeDiv.width = 10
        topNodeDiv.height = 10
        topNodeDiv.posX = 25
        topNodeDiv.posY = 2
        topNodeDiv.color = getColorForState(status.state)
        val witStr = HtmlUtil.witnessPath( status.witness )
        topNodeDiv.onClick = s"window.open('/logwindow/${status.track.trackName}/${status.goalName}/${witStr}')"
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
            val witStr = HtmlUtil.witnessPath( depGoalStatus.witness )
            depNode.onClick = s"window.open('/logwindow/${depGoalStatus.track.trackName}/${depGoalStatus.goalName}/${witStr}')"
            startX = depNode.posX
            pg.addConnection(depNode, currentNode)

            plumbGraphForStatusRecursive(pg, depGoalStatus, depNode, nodeMap)
        }

    }

    def readLogFile(track : TrackDescriptor, goalName: String, witness: Witness): Option[String] = {
        val logFile = LogWrapper.logPathForGoalWitness(track, goalName, witness)

        if ( LocalFileSystem.exists( logFile )) {
          ///// XXXX 
            Some( io.Source.fromFile(new java.io.File(logFile.toString)).getLines.mkString("\n"))
        } else {
            None
        }
    }
    
    def parseWitness( varString : String ) : Witness = {
      val vaSeq : Seq[VariableAssignment[String]] = varString.split(";").map( _.split("=") ).map( kvArr  => 
           { VariableAssignment[String](Variable( kvArr(0)), kvArr(1) ) } )
      
       Witness( vaSeq:_*)
    }
    
    /** 
     *   Convert a witness to a String which can be passed as a string in an URL
     */
    def witnessPath( witness : Witness ) : String = {
      witness.assignments.map( ass => {
           s"${ass.variable.name}=${ass.value}"
      } ).mkString(";")
    }
    
    /**
     * For a particular witness, display  a log window
     */
    def logWindow( trackName: String, goalName : String , varString : String  ) = Action {
       val witness = parseWitness( varString)
       
       val logFileOpt = readLogFile( TrackDescriptor( trackName), goalName, witness) 
        Ok(views.html.logwindow(trackName, goalName , witness, logFileOpt ))
       
    }
    
    /**
     *  Just output the logs as raw text ...
     */
    def rawLog( trackName: String, goalName : String , varString : String  ) = Action {
       val witness = parseWitness( varString)
       
       val logFileOpt = readLogFile( TrackDescriptor( trackName), goalName, witness) 
       
       logFileOpt match {
         case Some(logFile) =>
           Ok( logFile)
         
         case None =>
          NotFound(s" No Log File for $trackName $goalName $witness")
       } 
    }
    
    /**
     *  Abort a running job 
     */
    def abortJob( trackName: String, goalName : String , varString : String  ) = Action {
       val witness = parseWitness( varString)
       val goalOpt = getTrackGoalByName( trackName, goalName)
       goalOpt match {
            case Some(abortGoal) =>
             proofEngine.abortGoal(abortGoal._2, witness)
             ///currentStatusActiono       
             ///val statList = ProofEngine.getGoalsInProgress
             ///Ok(views.html.currentstatus( statList))
             Redirect( routes.SatisfyGoalPage.currentStatusAction)
            case None =>
              NotFound(s" No Goal found for $trackName $goalName $witness")
       }
    }
    
    def restartJob( trackName: String, goalName : String , varString : String  ) = Action {
       val witness = parseWitness( varString)
       val goalOpt = getTrackGoalByName( trackName, goalName)
       goalOpt match {
          case Some(abortGoal) =>
             proofEngine.restartGoal(abortGoal._2, witness)
             ///currentStatusActiono       
             Redirect( routes.SatisfyGoalPage.currentStatusAction)
          case None =>
              NotFound(s" No Goal found for $trackName $goalName $witness")
       }
    }
   
    /**
     *  Display a list of available log files for a given track
     */
    def logHistory( trackName : String ) = Action {
      val goalLogMap = LogWrapper.getGoalLogMap( trackName)
      
      Ok(views.html.loghistory(trackName, goalLogMap))
    }
    
    
    def goalProgress(trackName : String, goalName: String) = Action {
      
       getStatusForGoal( trackName, goalName )  match {
          case Some(status) => 
             status.progressCounter match {
               case Some(progress) =>
                 Ok(views.html.goalprogress(trackName, goalName, status.witness, progress ))
               case None =>
                 NotFound(s"Dude, can't determine progress for the goal ${goalName} in Track ${trackName}")
             }
            case None => 
              println( " NO STATUS FOUND -- DISPlaying history ")
              NotFound(s"Dude, we can't find the goal ${goalName} in Track ${trackName}")
       }
    }

    def getStatusForGoal(trackName: String, goalName: String): Option[GoalStatus] = {

        /// XXX TODO Push to ProofEngine ..
        //// use project name 
         ///val allStats = ProofEngine.getGoalsInProgress 
         ///allStats.foreach { stat => println( stat.track.name  + " :: " + stat.goal.name)}
        val statList = proofEngine.getGoalsInProgress.filter(_.track.trackName.equals(trackName)).filter(_.goalName.equals(goalName))
        if (statList.size > 0)
            Some(statList.head)
        else
            None
    }
    
    

    def getTrackGoalByName(trackName: String, goalName: String): Option[Tuple2[Track,Goal]] = {

        val track: Track = getTrackByName(trackName)
        ///println("Size of project allGoals is " + track.allGoals.size)
        println(track.allGoals)
        System.out.println(track.allGoals)
        info(s" Allgoals are ${track.allGoals}  GoalName = ${goalName} ")
        
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
        proofEngine.satisfyGoal( goal, witness)
        proofEngine.getStatus( goal, witness)
    }

    def getTrackByName(trackName: String): Track = {
        /// XXX Figure out how to use version and variant
        val trackDesc = TrackDescriptor( trackName)
        val trackOpt : Option[Track] = ProjectPage.trackFactory.getTrack( trackDesc)
        trackOpt.get
    } 

}