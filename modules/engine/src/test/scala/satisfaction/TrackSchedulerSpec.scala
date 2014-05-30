package com.klout
package satisfaction
package engine
package actors


/*
 * Tests for Scheduler
 */

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import satisfaction.fs._
import satisfaction.track.TrackFactory
import satisfaction.track.TrackScheduler
import scala.concurrent.Future
import org.joda.time.Period




@RunWith(classOf[JUnitRunner])
class TrackSchedulerSpec extends Specification {// val mockFS = new LocalFileSystem
  /*
 val resourcePath = LocalFileSystem.currentDirectory / "modules" / "engine" / "src" / "test" / "resources";
 val mockTrackFactory = new TrackFactory(mockFS, resourcePath / "user" / "satisfaction") //might not need this either
 
 implicit val hdfs : FileSystem = LocalFileSystem
 val engine = new ProofEngine()
 val scheduler = new TrackScheduler(engine)
 */ 
  
  
  //can be shared across spec
  val engine = new ProofEngine()
  val scheduler = new TrackScheduler(engine)
  ///this concludes set up
 
 "TrackSchedulerSpec" should {
 
   "schedule" in {
     "a reccuring job" in { // trackfactory hasa scheduler
    

       // possible variables that we can use
      	var x : Int = 0
        var observedVar = new Variable[String]("start", classOf[String])
        var expectedVar = new Variable[String]("changed", classOf[String])
        
        
        
        implicit val track : Track = new Track ( TrackDescriptor("scheduleRecurringTrack") ) with Recurring {  // might have bug; be careful (track properties might not be set; but we don't need it right now) 
         override def frequency = Recurring.period("P0Y0M0W0DT0H1M0S")
        }
      	
      	
        val vars: Set[Variable[_]] = Set(observedVar)
        val recurringGoal = TestGoal("RecurringGoal", vars)
       

      	track.addTopLevelGoal(recurringGoal)
      	
        scheduler.scheduleTrack(track)

     }
     
     "a cron job" in {
       
     }
     
   }//schedule
   
   
   "unschedule" in {
     "a reoccuring job" in {
       
     }
     
     "a cron job" in {
       
     }
     
   }// unschedule
   
   "list all current jobs" in {
     
   } //list all
   
 }//should
 
 
 //other functions
}