package willrogers
package controllers

import play.api._
import play.api.data._
import play.api.data.Forms._
import play.mvc.Controller
import play.api.mvc.Results._
import play.api.data.validation.Constraints._
import play.api.mvc.Action
import play.api.Configuration
import models.VariableHolder
import play.api.mvc
import com.klout.satisfaction._
import com.klout.satisfaction.engine._
import com.klout.satisfaction.engine.actors.ProofEngine
import com.klout.satisfaction.engine.actors._
import com.klout.satisfaction.track._
import com.klout.satisfaction.track.TrackHistory
import java.io.FileInputStream
import java.io.File
import models.PlumbGraph
import models.VariableFormHandler
import collection._
import play.mvc.Results


object TrackHistoryPage extends Controller {
  lazy val trackHistory = Global.trackHistory
  

  /**
   * default loader
   */
  def loadHistoryPageAction() = Action {
    val grList = trackHistory.getAllHistory
    val word = "OK"
   Ok(views.html.trackhistory(grList))
  }
  
  /**
   *  filter based on the desired Track/Goal, as well as start/end time
   */
  def filterHistory (trackName:String, forUser:String, version:String, variant:String, goalName:String, startTime:String, endTime:String) =  {
	  
	  println ("filterHistory: Here's what I got!")
	  println(" "+ trackName)
	  println(" "+ forUser)
	  println(" "+ version)
	  println(" "+ variant)
	  println(" "+ goalName)
	  println(" "+ startTime)
	  println(" "+ endTime)
	  
	  if (goalName.length == 0) println("  doesn't have goalName")
	  loadHistoryPageAction()
  }
  
  
  /**
   * look up all instances of a goal run
   * Following this tutorial: http://stackoverflow.com/questions/16857687/forms-in-scala-play-framework
   */
  
  val lookupGoalHistoryForm = Form(
      tuple(
          "trackName" -> text,
          "forUser" -> text,
          "version" -> text,
          "variant" -> text,
          "goalName"-> text,
          "witness" -> text
          ))
          
   def lookupJobHistoryGoal = Action { implicit request =>
    println("processing lookupGoalHistoryID submit action ")
    val(trackName, forUse, version, variant, goalName, witness) = lookupGoalHistoryForm.bindFromRequest.get
    
    val trackDesc = TrackDescriptor(trackName) //eh... might have to massage this part a bit more. Esp. string->Witness
    
    val grList = trackHistory.lookupGoalRun(trackDesc, goalName, null)
    Ok(views.html.trackhistory(grList))
  }
  
  /**
   * look up a specific goal run by ID
   */
	val lookupGoalHistoryIDForm = Form(
	    "runId" -> text
	)
	
    def lookupGoalHistoryID = Action { implicit request =>
    println(" processing lookupGoalHistoryID submit action")
    val runId= lookupGoalHistoryIDForm.bindFromRequest.get
    val gr = trackHistory.lookupGoalRun(runId)
    gr match {
      case Some(goal) => Ok(views.html.trackhistory(Seq(goal)))
      case None => Ok(views.html.trackhistory(Seq()))
    }
  }
}