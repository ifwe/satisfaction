package com.klout
package satisfaction
package track

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.joda.time.Period
import satisfaction.fs._
import satisfaction.track
import org.joda.time._
import satisfaction.engine.actors.GoalState
import org.joda.time.format.DateTimeFormatter
import java.text.SimpleDateFormat

@RunWith(classOf[JUnitRunner])
class TrackHistorySpec extends Specification {
  "TrackHistorySpec" should {
<<<<<<< HEAD
    //set ups
    val trackHistory = new JDBCSlickTrackHistory()
    val trackDesc : TrackDescriptor = TrackDescriptor ("testTrackNameDiff")
    val goalName : String = "testGoalName"
    val witness : Witness = null
    val dt : DateTime = new DateTime(System.currentTimeMillis())
=======
    val trackHistory = JDBCSlickTrackHistory
    
>>>>>>> upstream/master
    
    "insert started job into table" in  {
    
      val result :String = trackHistory.startRun(trackDesc, goalName, witness, dt)
      
     // H2DriverInfo.USER must be_==("sa") // NO
    }
    "update a running jobhistory" in { 
     //val result : String = trackHistory.completeRun("4", GoalState.Success)
    }
    
    "get Goals by time spans" in {
    	
      //set up custom start and end DateTimes
    	//val startTime : DateTime = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse("2014-06-13 15:43:07.672")
    	//val endTime = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse("2014-06-13 15:55:15.286")

        val resultList = trackHistory.goalRunsForGoal(trackDesc, goalName, None, None)
    }
    
<<<<<<< HEAD
    "look up goals" in {
      "by ID" in {
         val goalResult  = trackHistory.lookupGoalRun(1.toString)
        goalResult should not be (None)
        val goalResultFalse = trackHistory.lookupGoalRun(2000.toString)
        goalResultFalse should be (None) // this looks problematic, ask about better forms
      }
       "by desc" in {
         val goalListResult = trackHistory.lookupGoalRun(trackDesc, goalName, witness)
        //result2.size should_== 25
       }
=======
    "find GoalRun by ID" in {
        val goalResult  = trackHistory.lookupGoalRun("1")
        println(" Goal REsult = " + goalResult)
        
>>>>>>> upstream/master
    }
    
    "find all job runs"  in {
       val goalRuns = trackHistory.goalRunsForTrack(TrackDescriptor("DAU"), None, None)
       println(" Number of DAU Goals is " + goalRuns.size)
    }
  }
}