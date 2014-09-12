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
import org.joda.time.format.DateTimeFormat

@RunWith(classOf[JUnitRunner])
class TrackHistorySpec extends Specification {

  "TrackHistorySpec" should {
    //set ups
    val trackHistory =  JDBCSlickTrackHistory
    /*
    val trackDesc : TrackDescriptor = TrackDescriptor ("DAU")
    val goalName : String = "calcDAU"
    val witness : Witness = Witness( (Variable("date") -> "20140910" ),( Variable("hour") -> "02"))
    
    val dt : DateTime = new DateTime(System.currentTimeMillis())
*/
    
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    
    "inserting test tracks (DO THIS ONCE!!!)" in {
      val runId1 = trackHistory.startRun(TrackDescriptor("DAU"), "calcDAU", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:00:00"))
      val runId2 = trackHistory.startRun(TrackDescriptor("TestShell"), "testGoal", Witness( (Variable("date")->"20140909"), (Variable("hour")->"13")) , formatter.parseDateTime("2014-09-09 14:01:00"))
      val runId3 = trackHistory.startSubGoalRun(TrackDescriptor("DAU"), "page_view", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:03:00"), 1.toString())
      val runId4 = trackHistory.startRun(TrackDescriptor("Userdata_light"), "calcUDL", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:09:00"))
      val runId5 = trackHistory.startSubGoalRun(TrackDescriptor("Userdata_light"), "Shell", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:10:00"), 4.toString())
      val runId6 = trackHistory.startSubGoalRun(TrackDescriptor("Userdata_light"), "Agg", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:11:00"), 4.toString())
      val runId7 = trackHistory.startSubGoalRun(TrackDescriptor("DAU"), "login_detail", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:05:00"), 1.toString())
      val runId8 = trackHistory.startSubGoalRun(TrackDescriptor("Userdata_light"), "Shell", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:20:00"), 4.toString())
      val runId9 = trackHistory.startSubGoalRun(TrackDescriptor("Userdata_light"), "Agg", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:21:00"), 4.toString())
      val runId10 = trackHistory.startSubGoalRun(TrackDescriptor("Userdata_light"), "Shell", Witness( (Variable("date")->"20140908")) , formatter.parseDateTime("2014-09-09 14:30:00"), 4.toString())
      val runId11 = trackHistory.startRun(TrackDescriptor("TestShell"), "testGoal", Witness( (Variable("date")->"20140909"), (Variable("hour")->"14")) , formatter.parseDateTime("2014-09-09 15:00:00"))
      val runId12 = trackHistory.startRun(TrackDescriptor("TestShell"), "testGoal", Witness( (Variable("date")->"20140909"), (Variable("hour")->"14")) , formatter.parseDateTime("2014-09-09 15:10:00"))
      val runId13 = trackHistory.startRun(TrackDescriptor("Userdata_light"), "calcUDL", Witness( (Variable("date")->"20140907")) , formatter.parseDateTime("2014-09-09 15:50:00"))
      val runId14 = trackHistory.startSubGoalRun(TrackDescriptor("Userdata_light"), "Shell", Witness( (Variable("date")->"20140907")) , formatter.parseDateTime("2014-09-09 15:51:00"), 13.toString())
      val runId15 = trackHistory.startSubGoalRun(TrackDescriptor("Userdata_light"), "Agg", Witness( (Variable("date")->"20140907")) , formatter.parseDateTime("2014-09-09 15:53:00"), 13.toString())
      val runId16 = trackHistory.startRun(TrackDescriptor("Userdata_light"), "calcUDL", Witness( (Variable("date")->"20140907")) , formatter.parseDateTime("2014-09-09 16:15:00"))
      val runId17 = trackHistory.startRun(TrackDescriptor("NestTrack"), "nestGoal", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:16:00"))
      val runId18 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "TaskA", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:17:00"), 17.toString())
      val runId19 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "SubTaskA1", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:18:00"), 18.toString())
      val runId20 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "SubTaskA2", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:19:00"), 18.toString())
      val runId21 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "TaskB", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:20:00"), 17.toString())
      val runId22 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "SubTaskB1", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:21:00"), 21.toString())
      val runId23 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "SubTaskB2", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:22:00"), 21.toString())
      val runId24 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "SubTaskB3", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:23:00"), 21.toString())
      val runId25 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "SubsubTaskB3A", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:24:00"), 24.toString())
      val runId26 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "SubsubTaskB3B", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:25:00"), 24.toString())
      val runId27 = trackHistory.startSubGoalRun(TrackDescriptor("NestTrack"), "TaskC", Witness( (Variable("date")->"20140909")) , formatter.parseDateTime("2014-09-09 16:26:00"), 17.toString())
    }
   
    "show all tracks" in {
      
      val resultList = trackHistory.getAllHistory
      resultList.foreach(gr => gr.printGoalRun)
    }
    /*
    "insert started job into table" in {
    
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