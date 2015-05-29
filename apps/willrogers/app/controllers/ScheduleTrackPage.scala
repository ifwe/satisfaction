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
import satisfaction._
import satisfaction.engine._
import satisfaction.engine.actors.ProofEngine
import satisfaction.engine.actors._
import satisfaction.track._
import java.io.FileInputStream
import java.io.File
import models.PlumbGraph
import models.VariableFormHandler
import collection._
import play.mvc.Results
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 *   Page for seeing which Tracks have been scheduled,
 *    and for seeing which tracks are not scheduled.
 */
object ScheduleTrackPage extends Controller {
     println(" must show schedules!")

   lazy val trackFactory =  Global.trackFactory 
   lazy val scheduler = Global.trackScheduler
   
   
   def showSchedulesAction() = Action { 
    	 val scList = scheduler.getScheduledTracks
    	 val tdList = trackFactory.getAllTracks
    	 //val tdList = Seq()
    
     	Ok(views.html.scheduletrack(tdList.toSeq, scList.toSeq))
   }
     

   
   /**
    * functions for scheduling a specific track.
    */
   
   val scheduleTrackForm = Form {
     tuple(
         "rule" -> text,
         "pattern" -> text,
         "stoppable" -> text
         )
   }
   def scheduleTrack (trackName: String, forUser: String, version: String) = Action { implicit request =>
     
     
    val (rule, pattern, stoppable) = scheduleTrackForm.bindFromRequest.get
    
         println("scheduleTrack: trackName is " + trackName + " rule is " + rule + " pattern is " + pattern + " stoppable is " + stoppable)

    
    val holderTrack: Track= {
      rule match {
        case cron if rule.contains("cron") =>
          new Track(TrackDescriptor(trackName)) with Cronable {
            override def cronString = pattern
          }
        case rec if rule.contains("recur") =>
          new Track(TrackDescriptor(trackName)) with Recurring {
            override def frequency = Recurring.period(pattern)
          }
        case const if rule.contains("constantly") => 
           new Track(TrackDescriptor(trackName)) with Constantly {
             override def retryOnFailure = true
          }
      }
    }
    val pausable : Boolean = {
      stoppable match {
        case yes if (rule.contains("constantly") || stoppable.contains("pause") || stoppable.contains("kill")) =>
          true
        case _ => 
          false
      }
    }
    
    
   val result =    scheduler.scheduleTrack(holderTrack) 
    
    val scList = scheduler.getScheduledTracks.toSeq
    ///val tdList = trackFactory.getAllTracks.diff(scList)
    val tdList = Seq()

   
    result match {
     
     case Success(message) =>
     	Ok(views.html.scheduletrack(tdList, scList, Some(message)))
     case Failure(reason) =>
     	Ok(views.html.scheduletrack(tdList, scList, Some(reason.getMessage)))
    }
      
   }
   
   def unscheduleOneTrack(trackName: String, forUser:String, version:String) = Action {
     
     val wtf = scheduler.getScheduledTracks.map(_._1).map(td => println("      unscheduleOneTrack found:  " + td.trackName + " " + td.forUser + " " + td.version + " " + td.variant))
     val desc = scheduler.getScheduledTracks.filter(_._1.trackName == trackName ).last._1
     
     
     scheduler.unscheduleTrack(desc) 
      println(" willrogers scheduler - I should be unscheduled!")

     val scList = scheduler.getScheduledTracks.map(_._1).toSeq
     val tdList = trackFactory.getAllTracks.toSeq.diff(scList)
     //Ok(views.html.scheduletrack(tdList, scList)) 
     Ok(s"track "+trackName+" unscheduled")
   }
}
