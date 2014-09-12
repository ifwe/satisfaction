package willrogers
package controllers

import java.text.SimpleDateFormat

import scala.collection.Seq

import org.joda.time.DateTime

import play.api.data.Form
import play.api.data.Forms.text
import play.api.data.Forms.tuple
import play.api.mvc.Action
import play.api.mvc.Results.Ok
import play.mvc.Controller
import satisfaction.TrackDescriptor
import willrogers.Global



object TrackHistoryPage extends Controller {
  lazy val trackHistory = Global.trackHistory
  
  //private var grList = trackHistory.getAllHistory
  var grList = trackHistory.getRecentHistory // by default - only grab recent tracks

  /**
   * default loader
   */
  def loadHistoryPageAction() = Action {

    println("loading page history: I have "+ grList.length + " tracks")
   Ok(views.html.trackhistory(grList))
   
  }
  
  /**
   *  filter based on the desired Track/Goal, as well as start/end time
   */
  
    val filterHistoryForm = Form(
      tuple(
          "trackName" -> text,
          "forUser" -> text,
          "version" -> text,
          "variant" -> text,
          "goalName"-> text,
          "startTime" -> text,
          "endTime" -> text
          ))
          
  def filterJobHistory = Action { implicit request =>
	  val(trackName, forUser, version, variant, goalName, startTime, endTime) = filterHistoryForm.bindFromRequest.get
	  
	  //set up variables - need to massage this part....
	  val trackDesc : TrackDescriptor = TrackDescriptor (trackName)
	  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") 
	  val sTime : Option[DateTime]= startTime match{
	    case timestring if timestring.length() > 0 => Some(new DateTime(simpleDateFormat.parse(timestring)))
	    case _ => None
	  }
	  
	  val eTime: Option[DateTime]=endTime match{
	    case timestring if timestring.length() > 0 => Some(new DateTime(simpleDateFormat.parse(timestring)))
	    case _ => None
	  }
	  
	  
	  val grList = goalName match {
	    case name if name.length() > 0 => trackHistory.goalRunsForGoal(trackDesc, goalName, sTime, eTime)
	    case _ => trackHistory.goalRunsForTrack(trackDesc, sTime, eTime)
	  }
	  
	 Ok(views.html.trackhistory(grList))
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
    val(trackName, forUser, version, variant, goalName, witness) = lookupGoalHistoryForm.bindFromRequest.get
    
    val trackDesc = TrackDescriptor(trackName) //FixME: eh... might have to massage this part a bit more. Esp. String->Witness
    
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
    val runId= lookupGoalHistoryIDForm.bindFromRequest.get
    val gr = trackHistory.lookupGoalRun(runId)
    gr match {
      case Some(goal) => Ok(views.html.trackhistory(Seq(goal)))
      case None => Ok(views.html.trackhistory(Seq()))
    }
  }
	
	
	/**
	 * If you want to see everything that ever ran
	 */
	def getAllHistoryRuns() = Action {
		//val grList = trackHistory.getAllHistory
		grList=trackHistory.getAllHistory
				Ok(views.html.trackhistory(grList))
	}
}
