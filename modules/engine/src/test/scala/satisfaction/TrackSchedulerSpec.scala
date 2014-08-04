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
import satisfaction.Temporal
import org.joda.time.DateTime
import org.joda.time.LocalTime





@RunWith(classOf[JUnitRunner])
class TrackSchedulerSpec extends Specification {// val mockFS = new LocalFileSystem

 val mockFS = new LocalFileSystem
 val resourcePath = LocalFileSystem.currentDirectory / "modules" / "engine" / "src" / "test" / "resources";
  
  //can be shared across spec
  val engine = new ProofEngine()
  val scheduler = new TrackScheduler(engine)

 
  ///this concludes set up
 
 
 "TrackSchedulerSpec" should {
    
    /*
     * Things we must test:
     * - schedule a simple recurring job
     * - schedule a simple cron job
     * - schedule r/c job that have dependencies
     * - unschedule jobs
     * - list all jobs in scheduler
     * - generate witness (non-temporal case) needs to be covered
     */
 
   "schedule" in {
   "a single recuring job" in { 
    
       // possible variables that we can test on
      	var x : Int = 1
        var oldValue: Int = 0 
        
        //set up track
        implicit val track : Track = new Track ( TrackDescriptor("scheduleRecurringTrack") )  with Recurring  {  // might have bug; be careful (track properties might not be set; but we don't need it right now) 
    	 //P0Y0M0W0DT0H0M3S P1Y2M3W4DT5H6M7.008S PT1M
      	  override def frequency = Recurring.period("P0Y0M0W0DT1H0M0S") //stick with the standard format! PyYmMwWdDThHmMsS
      	  override def timeOffset = Some(new LocalTime( 0, 27))
      	  
      	  override def init = {
            val recurringGoal : Goal = {
        	  //first, define custom satisfier
        	  val satisfier = new Satisfier() {
        	    override def name="recurringMockSatisfier"
  
        	    override def satisfy (witness:Witness) : ExecutionResult = robustly { // redefine satisfier
        	      println(" Satisfier getting called at " + DateTime.now)
        	      if (x == oldValue + 1) {
        	        oldValue = x // replaces anon fn
        	        Thread.sleep(3000)
        	        x+=1
        	        true
        	      } else {
        	        false
        	      }
        	    } 
        	    
        	    override def abort()  = { null}
        	  }
      	      val observedVar = new Variable[String]("timeLapse", classOf[String]) with TemporalVariable { // if not temporal: we will have to define a property file
                 override val formatString = "YYYYMMDD hh:mm:ss"
                 override val frequency = Temporal.frequency("PT1S")
      	      }
              val vars: List[Variable[_]] = List(observedVar)

 
        	  //now set up a goal
        	  var rg = new Goal("RecurringGoal", Some(satisfier), vars)
        	  rg
      	  }.declareTopLevel

      	  }
      	}
      	
      	
      	val trackFactory : TrackFactory = {
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


 
        scheduler.scheduleTrack(track)
        
        Thread.sleep(60*1000*3)
        println(" x is equal to " + x)
        x mustEqual 3
        /*
        while (true) {
          println("main thread: x is now "+ x)
          x mustEqual oldValue + 1
          Thread.sleep(65000)
        }
        * 
        */
     }
     
     "a single cron job" in {
       /*
       // possible variables that we can test on
      	var x : Int = 1
        var oldValue: Int = 0 
       
       //set up track
       implicit val track: Track = new Track ( TrackDescriptor("scheduleCronTrack") ) with Cronable {
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
       //val cronGoal = TestGoal("CronGoal", vars)
       implicit val cronGoal : Goal = {
          try {
        	  //first, define custom satisfier
        	  val satisfier = new MockSatisfier() {
        	    override def name="cronMockSatisfier"
  
        	    override def satisfy (witness:Witness) : ExecutionResult = robustly { // redefine satisfier
        	      if (x == oldValue + 1) {
        	        oldValue = x // replaces anon fn
        	        x+=1
        	        true
        	      } else {
        	        false
        	      }
        	    } 
        	  }
        	  //now set up a goal
        	  var rg = new Goal("CronGoal", Some(satisfier), vars)
        	  rg
          } catch{
             case unexpected: Throwable =>
		        unexpected.printStackTrace(System.out) 
		        throw unexpected
          }
        }
       
       track.addTopLevelGoal(cronGoal)
       scheduler.scheduleTrack(track)
       
       Thread.sleep(60000)
       println("main thread: cron x is now is now "+ x)
       x mustEqual 2
        
        
//       while (true) {
//          Thread.sleep(60000)
//          println("main thread: cron x is now is now "+ x)
//          x mustEqual oldValue + 1
//       }
       */
     }
     
   }//schedule
   
   
   "unschedule" in {
     "a recuring job" in {
       /*
       implicit val track: Track = new Track (TrackDescriptor("scheduleRecurringTrack")) with Recurring {
      	  override def frequency = new Period(0,0,0,0,0,0,1,0) //Recurring.period("PT1S")
       }
       implicit val trackFactory : TrackFactory = {
         try {
           var tf = new TrackFactory ( mockFS, resourcePath, Some(scheduler)) {
             override def getTrack(trackDesc: TrackDescriptor): Option[Track] = {
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
       
       var observedVar = new Variable[String]("time", classOf[String]) with TemporalVariable {
         override val FormatString = "YYYYMMDD hh:mm:ss"
       }
       val vars: List[Variable[_]] = List(observedVar)
       val recurringGoal = TestGoal("RecurringGoal", vars)
       track.addTopLevelGoal(recurringGoal)
       scheduler.scheduleTrack(track)
       
       Thread.sleep(5000)       
       scheduler.unscheduleTrack(track.descriptor)
       scheduler.getScheduledTracks must haveSize(0)
       * 
       */
     }
     "a cron job" in { 
       /*
      // WARNING: this test takes 2 minutes to run due to the granularity of cron jobs 
      implicit val track: Track = new Track ( TrackDescriptor("scheduleCronTrack") ) with Cronable {
         override def cronString = "0 0/1 * 1/1 * ? *"
       }
       implicit val trackFactory : TrackFactory = {
         try {
           var tf = new TrackFactory ( mockFS, resourcePath, Some(scheduler)) {
             override def getTrack(trackDesc: TrackDescriptor): Option[Track] = {
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
       
       var observedVar = new Variable[String]("minutes", classOf[String]) with TemporalVariable {
         override val FormatString = "mm"
       }
       val vars: List[Variable[_]] = List(observedVar)
       val recurringGoal = TestGoal("RecurringGoal", vars)
       track.addTopLevelGoal(recurringGoal)
       scheduler.scheduleTrack(track)
       
       Thread.sleep(125000)
       println(" scheduler had "+scheduler.getScheduledTracks.size+" tracks scheduled")

       scheduler.unscheduleTrack(track.descriptor)
       println(" scheduler should have "+scheduler.getScheduledTracks.size+" tracks scheduled")
       scheduler.getScheduledTracks must haveSize(0)
       */
     }
   }// unschedule
   
   "list all current jobs" in {
     /*
     implicit val cronTrack: Track = new Track ( TrackDescriptor("scheduleCronTrack") ) with Cronable {
         override def cronString = "0 0/1 * 1/1 * ? *"
     }
     var cronVar = new Variable[String]("minutes", classOf[String]) with TemporalVariable {
         override val formatString = "mm"
         override val frequency = Temporal.minutePeriod
     }
     val cronVars : List[Variable[_]] = List(cronVar)
     val cronGoal = TestGoal("cronGoal", cronVars)
     cronTrack.addTopLevelGoal(cronGoal)
     
     //test sizeof returned set
     scheduler.getScheduledTracks must haveSize(0)
     
     scheduler.scheduleTrack(cronTrack)
     scheduler.getScheduledTracks must haveSize(1)
     
     scheduler.unscheduleTrack(cronTrack.descriptor)
     scheduler.getScheduledTracks must haveSize(0)
     */
   } //list all
   
 }//should
 
 
 //other functions
}