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
import play.api.mvc.Call
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
  
  def loadHistoryPageAction() = Action {
    val grList = trackHistory.getAllHistory
    val word = "OK"
      println( "ahhhhh sizeof grList is " + grList.length)
   Ok(views.html.trackhistory(grList))
  }
  
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
}