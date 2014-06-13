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

@RunWith(classOf[JUnitRunner])
class TrackHistorySpec extends Specification {
  "TrackHistorySpec" should {
    val trackHistory = JDBCSlickTrackHistory
    
    
    "insert started job into table" in  {
      val trackDesc : TrackDescriptor = TrackDescriptor ("testTrackName")
      val goalName : String = "testGoalName"
      val witness : Witness = null
      val dt : DateTime = new DateTime(System.currentTimeMillis())
      
      val result :String = trackHistory.startRun(trackDesc, goalName, witness, dt)
      val result2 = trackHistory.lookupGoalRun(trackDesc, goalName, witness)
      result  must have length 1 // NO
      
      
     // H2DriverInfo.USER must be_==("sa") // NO
    }
    
    "find GoalRun by ID" in {
        val goalResult  = trackHistory.lookupGoalRun("1")
        println(" Goal REsult = " + goalResult)
        
    }
    
    "find all job runs"  in {
       val goalRuns = trackHistory.goalRunsForTrack(TrackDescriptor("DAU"), None, None)
       println(" Number of DAU Goals is " + goalRuns.size)
    }
  }
}