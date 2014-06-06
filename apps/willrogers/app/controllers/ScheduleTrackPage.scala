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
   def showSchedulesAction = Action { 
   
       val tdList = trackFactory.getAllTracks
      
     	Ok(views.html.scheduletrack(tdList))
   }
  
}