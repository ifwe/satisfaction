package satisfaction
package track

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.joda.time.Period
import satisfaction.fs._
import satisfaction.track
import org.joda.time._
import org.joda.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import Witness2Json._
import satisfaction.track.TrackHistory._

@RunWith(classOf[JUnitRunner])
class TrackHistorySpec extends Specification {

  "TrackHistorySpec" should {
    //set ups
    val trackHistory =  JDBCSlickTrackHistory
    val trackDesc : TrackDescriptor = TrackDescriptor ("testTrackName")
    val goalName : String = "testGoalName"
    val witness : Witness = Witness( (Variable("date") -> "20140522" ),( Variable("hour") -> "02"))
    
    val dt : DateTime = new DateTime(System.currentTimeMillis())

   
    
    "insert started job into table" in  {
    
      val runId  = trackHistory.startRun(trackDesc, goalName, witness, dt)
      println(" inserted string " + runId)
      val goalResult  = trackHistory.lookupGoalRun(runId.toString).get
      
      println(" GoalResult is " + goalResult)
      goalResult.state must_== GoalState.Running
      

      //result.toString must be 
     // H2DriverInfo.USER must be_==("sa") // NO
    }
     
    "update a running jobhistory" in { 
     //val result : String = trackHistory.completeRun("29", GoalState.Success)

      val runId  = trackHistory.startRun(trackDesc, goalName, witness, dt)
      println(" inserted string " + runId)
      
      val goalResult  = trackHistory.lookupGoalRun(runId.toString).get
      println(" Result 1 is " + goalResult)
      goalResult.state must_== GoalState.Running

      goalResult.endTime must beNone
      
      val completeRun = trackHistory.completeRun( runId, GoalState.Success)

      val goalResult2 = trackHistory.lookupGoalRun(runId.toString).get
      println(" Result 2 is " + goalResult2)
      goalResult2.state must_== GoalState.Success
      
      goalResult2.endTime must not beNone
      //println(" Result 2 endtime  is " + goalResult2.endTime.get)

    }
    
    /**
    "get Goals by time spans" in {
    	
      //set up custom start and end DateTimes; then toggle 
    	val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    	val startTime = new DateTime(simpleDateFormat.parse("2014-06-13 15:43:07"))
    	val endTime = new DateTime(simpleDateFormat.parse("2014-06-17 15:55:15"))

        val resultList = trackHistory.goalRunsForGoal(trackDesc, goalName, None, Some(endTime))
    }

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
    }
  }
  
  
  "Translate a Witness to JSON" should {
      "Produce JSON " in {
         val witness = Witness( VariableAssignment( Variable("date"), "20140522"), VariableAssignment(Variable("hour"), "08"))
         val json = renderWitness( witness)
         
         json.length must_!= 0
         println(json)
        
      } 
      
      "Parse JSON" in {
         val jsonStr =  "{\"date\":\"20140522\",\"hour\":\"08\"}"
           
         val witness = parseWitness(jsonStr)
         
         val year = witness.get(Variable("date")).get
         
         year must_== "20140522"
           
         val hour = witness.get(Variable("hour")).get
         
         hour must_== "08"
           
      }

**/
      
    
  }
  
}