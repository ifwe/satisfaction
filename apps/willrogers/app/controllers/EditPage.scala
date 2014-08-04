package willrogers
package controllers

import play.api._
import play.api.mvc._
import Results._
import play.api.libs.concurrent.Execution.Implicits._
import scala.concurrent.duration._
import satisfaction.Track
import satisfaction.Goal
import models.PlumbGraph
import models._
import collection._
import satisfaction.engine.actors.GoalStatus
import satisfaction.track._
import satisfaction.TrackDescriptor

object EditPage extends Controller {
  val trackFactory : TrackFactory = Global.trackFactory

  
    def listFiles( trackName : String ) = Action {
       val trackDesc = TrackDescriptor( trackName)
       val trackOpt : Option[Track] = trackFactory.getTrack( trackDesc)
       trackOpt match {
         case Some(track) =>
           ////val files = track.listResources.map( _.getName ).toList
           ///val files = track.listResources.toList
           val files = getResources(track)
           
           
           Ok(views.html.listfiles(trackName, files )) 
         case None => 
          Ok( views.html.brokenproject( trackName))
       }
    
    }
  
  
    def getResources( track : Track) : List[String] = {
      ///trackFactory.trackFS.listFiles( track.trackPath / "resource" ).map( _.getPath.toUri.getPath.split("/").last).toList
      track.listResources.map( _.split("/").last).toList
      
    }
  
    def editFile(trackName: String, resourceName : String) = Action {
        val trackDesc = TrackDescriptor( trackName)
        val trackOpt : Option[Track] = trackFactory.getTrack( trackDesc)
        trackOpt match {
          case Some(track) =>
            val resourceContents = track.getResource( resourceName)

            Ok(views.html.hiveeditor(trackName, resourceName, resourceContents))
        case None =>
          Ok( views.html.brokenproject( trackName))
        }

    }


}
