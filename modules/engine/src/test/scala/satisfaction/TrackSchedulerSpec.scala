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



@RunWith(classOf[JUnitRunner])
class TrackSchedulerSpec extends Specification {// val mockFS = new LocalFileSystem
  /*
 val resourcePath = LocalFileSystem.currentDirectory / "modules" / "engine" / "src" / "test" / "resources";
 val mockTrackFactory = new TrackFactory(mockFS, resourcePath / "user" / "satisfaction") //might not need this either
 
 implicit val hdfs : FileSystem = LocalFileSystem
 val engine = new ProofEngine()
 val scheduler = new TrackScheduler(engine)
 */ 
  ///this concludes set up
 
 "TrackSchedulerSpec" should {
 
   "schedule" in {
     "a reoccuring job" in { // trackfactory hasa scheduler
       /*
        implicit val track: Track = new Track( TrackDescriptor("TestSchedulerTrack")) with Recurring { // might have bug; be careful (track properties might not be set; but we don't need it right now)
		   override def frequency = Recurring.period("3m")
		   println("created track with recurring trait")
		 }
        */
        val engine = new ProofEngine()
        val observedVar = new Variable[String]("string", classOf[String])
        val vars: List[Variable[_]] = List(observedVar)
        val recurringGoal = TestGoal("RecurringGoal", vars)
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