package controllers

import play.api._
import play.api.mvc._
import Results._
import play.api.libs.concurrent.Execution.Implicits._
import scala.concurrent.duration._
import com.klout.satisfaction.Track
import com.klout.satisfaction.Goal
import models.PlumbGraph
import models._
import com.klout.satisfaction.executor.actors.GoalStatus
import collection._
import com.klout.satisfaction.executor.track._
import com.klout.satisfaction.TrackDescriptor

object EditPage extends Controller {
  val trackFactory : TrackFactory = TrackFactory

  
    def listFiles( trackName : String ) = Action {
       val trackDesc = TrackDescriptor( trackName)
       val trackOpt : Option[Track] = trackFactory.getTrack( trackDesc)
       trackOpt match {
         case Some(track) =>
           val files = track.listResources.map( _.getName ).toList
           Ok(views.html.listfiles(trackName, files )) 
         case None => 
          Ok( views.html.brokenproject( trackName))
       }
    
    }
  
    def editFile(trackName: String, resourceName : String) = Action {
        ///val project = SyncApi.getProject(projName)
       //// Display only major projects 
        val trackDesc = TrackDescriptor( trackName)
        val trackOpt : Option[Track] = trackFactory.getTrack( trackDesc)
        //val internalGoals = project.project.get.internalGoals.toList
        //val externalGoals = project.project.get.externalGoals.toList
        ///val internalGoals = project.topLevelGoals.toList
        trackOpt match {
          case Some(track) =>
            val resourceContents = track.getResource( resourceName)

            Ok(views.html.hiveeditor(trackName, resourceName, resourceContents))
        case None =>
          Ok( views.html.brokenproject( trackName))
        }

    }


}
