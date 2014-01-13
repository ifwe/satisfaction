package com.klout
package controllers

import play.api._
import play.api.data._
import play.api.data.Forms._
import play.mvc.Controller
import play.api.mvc.Results._
import play.api.data.validation.Constraints._
import play.api.mvc.Action
import models.VariableHolder
import play.api.mvc.Call
import com.klout.satisfaction._
import satisfaction.engine._
import satisfaction.engine.actors.ProofEngine
import satisfaction.engine.actors._
import com.klout.satisfaction.executor.track._
import java.io.FileInputStream
import java.io.File
import models.PlumbGraph
import models.VariableFormHandler
import collection._
import play.mvc.Results

/**
 *   Page for seeing which Tracks have been scheduled,
 *    and for 
 *   
 */
object ScheduleTrackPage extends Controller {
   lazy val trackFactory = new TrackFactory( new java.net.URI("hdfs://jobs-dev-hnn"),
		   	"/user/satisfaction")

  
   def showSchedulesAction( ) = Action { 
   
       val tdList = trackFactory.getAllTracks
     	Ok(views.html.showschedule(tdList))
   }

}