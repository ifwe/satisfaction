package com.klout
package satisfaction
package engine
package actors

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.joda.time.Period

import satisfaction.fs._
import satisfaction.track.TrackFactory
import satisfaction.track.TrackScheduler





@RunWith(classOf[JUnitRunner])
class TrackSchedulerSpec extends Specification {// val mockFS = new LocalFileSystem

 val mockFS = new LocalFileSystem
 val resourcePath = LocalFileSystem.currentDirectory / "modules" / "engine" / "src" / "test" / "resources";

 
  /*
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
     "a reccuring job" in { 
    

       // possible variables that we can use
      	var x : Int = 0
        var oldValue: Int = x
        
        //set up track
        implicit val track : Track = new Track ( TrackDescriptor("scheduleRecurringTrack") ) with Recurring {  // might have bug; be careful (track properties might not be set; but we don't need it right now) 
        	//P0Y0M0W0DT0H0M3S
      	  //P1Y2M3W4DT5H6M7.008S
      	  override def frequency = Recurring.period("P0Y0M0W0DT0H0M3S")
        }
      	
        //set up mock trackFactory
      	implicit val trackFactory : TrackFactory = {
      	  try{
	      	  //val hadoopWitness: Witness = Config.Configuration2Witness(Config.config)
		      var tf = new TrackFactory( mockFS, resourcePath, Some(scheduler)) {
		        override def getTrack(trackDesc : TrackDescriptor) : Option[Track] = {
		          Some(track)
		        }
		      }
		      scheduler.trackFactory = tf
		      tf  
      	  } catch {
      	    case unexpected: Throwable =>
      	      unexpected.printStackTrace(System.out)
      	      throw unexpected
      	  }
      	}

      	var observedVar = new Variable[String]("minute", classOf[String]) with TemporalVariable { // if not temporal: we will have to define a property file
          override val FormatString = "mm"
       }
        val vars: List[Variable[_]] = List(observedVar)
        val recurringGoal = TestGoal("RecurringGoal", vars)
       

      	track.addTopLevelGoal(recurringGoal)
      	
        scheduler.scheduleTrack(track)

     }
     
     "a cron job" in {
       /*
       var x : Int =0
       var oldValue: Int = x
       
       //set up track
       implicit val track: Track = new Track ( TrackDescriptor("scheduleChronTrack") ) with Cronable {
         override def cronString = "0 0/1 * 1/1 * ? *"
       }
       
       //set up mock trackFactory
       implicit val trackFactory : TrackFactory = { // taken from willrogers
	    try {
	      //val hadoopWitness: Witness = Config.Configuration2Witness(Config.config)
	      var tf = new TrackFactory( mockFS, resourcePath, Some(scheduler)) {
	        override
	        def getTrack(trackDesc : TrackDescriptor) : Option[Track] = {
	          Some(track)
	        }
	      }
	      scheduler.trackFactory = tf
	      
	
	      tf
	    } catch {
	      case unexpected: Throwable =>
	        unexpected.printStackTrace(System.out) 
	        throw unexpected
	    }
	  }
       
       
       var observedVar = new Variable[String]("minute", classOf[String]) with TemporalVariable { // if not temporal: we will have to define a property file
          override val FormatString = "mm"
       }
 
       val vars: List[Variable[_]]=List(observedVar)
       val cronGoal = TestGoal("CronGoal", vars)
       
       track.addTopLevelGoal(cronGoal)
       scheduler.scheduleTrack(track)
    
       */
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