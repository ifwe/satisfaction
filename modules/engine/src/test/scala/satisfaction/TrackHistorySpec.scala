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

@RunWith(classOf[JUnitRunner])
class TrackHistorySpec extends Specification {
  "TrackHistorySpec" should {
    //set ups
    val trackHistory = new JDBCSlickTrackHistory()
    val trackDesc : TrackDescriptor = TrackDescriptor ("testTrackName")
    val goalName : String = "testGoalName"
    val witness : Witness = null
    val dt : DateTime = new DateTime(System.currentTimeMillis())
    
    "insert started job into table" in  {
    
      val result :String = trackHistory.startRun(trackDesc, goalName, witness, dt)

     // H2DriverInfo.USER must be_==("sa") // NO
    }
    "update a running jobhistory" in {
      val result : String = trackHistory.completeRun("2", GoalState.Success)
    }
    
    "look up goal(s)" in {
        val result2 = trackHistory.lookupGoalRun(trackDesc, goalName, witness)
    }
    
    "find GoalRun by ID" in {
        val goalResult  = trackHistory.lookupGoalRun(1.toString)
        //found
        //val goalResultFalse = trackHistory.lookupGoalRun(2000.toString)
        //not found
    }
  }
}