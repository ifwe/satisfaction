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
import java.io.FileInputStream
import java.io.File
import models.PlumbGraph
import models.VariableFormHandler
import collection._
import play.mvc.Results

/**
 *   Page for seeing which Tracks have been scheduled,
 *    and for seeing which tracks are not scheduled.
 */
object ScheduleTrackPage extends Controller {
     println(" must show schedules!")

   lazy val trackFactory =  Global.trackFactory 
   lazy val scheduler = Global.trackScheduler
   
   
   def showSchedulesAction() = Action { 
    	 val scList = scheduler.getScheduledTracks.map(_._1).toSeq
       val tdList = trackFactory.getAllTracks.diff(scList)
    
     	Ok(views.html.scheduletrack( tdList, scList))
   }
     
   def scheduleOneTrack(trackName: String, rule: String, pattern: String) = Action {

     println("my rule is "+rule)
    implicit val holderTrack: Track= {
      rule match {
        case cron if rule.contains("cron") =>
          new Track(TrackDescriptor(trackName)) with Cronable {
            override def cronString = pattern
          }
        case rec if rule.contains("recur") =>
          new Track(TrackDescriptor(trackName)) with Recurring {
            override def frequency = Recurring.period(pattern)
          }
      }
    } 
      scheduler.scheduleTrack(holderTrack)
           println(" I've been scheduled!")

     // Later: might want to add some live feedback for to the associated views....
       val scList = scheduler.getScheduledTracks.map(_._1).toSeq
       val tdList = trackFactory.getAllTracks.diff(scList)
     Ok(views.html.scheduletrack(tdList, scList)) //FIXME: Reload, not new page
   }
   
   def unscheduleOneTrack(trackName: String) = Action {
     val desc = scheduler.getScheduledTracks.filter(_._1.trackName == trackName).last._1
     scheduler.unscheduleTrack(desc) //YY this is gonna be broken due to versioning!
      println(" I've been unscheduled!")

     val scList = scheduler.getScheduledTracks.map(_._1).toSeq
     val tdList = trackFactory.getAllTracks.diff(scList)
     Ok(views.html.scheduletrack(tdList, scList)) 
   }
}