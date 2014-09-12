package satisfaction
package track

import org.joda.time._
import GoalStatus._

/**
 *  Interface to a persistent DB implementation which will store 
 *    track run history.
 */
trait TrackHistory {
  /**
   *  Case class representing a single 
   */
  case class GoalRun( val trackDescriptor : TrackDescriptor,
         val goalName : String, 
         val witness : Witness, 
         val startTime : DateTime, 
         val endTime : Option[DateTime],
         val state : GoalState.State,
         val parentRunId: Option[String]) {
    
     /**
      *  DB Identifier for the run
      */
     var runId : String = null
     
     def printGoalRun = {
       val formatted : String = "A goalRun trackName: " + trackDescriptor.trackName +
    		   			" goalName: " + goalName +
    		   			" witness: " + witness +
    		   			" startTime: " + startTime + 
    		   			" endTime: " + endTime + 
    		   			" state: " + state +
    		   			" parentID: " + parentRunId
       println(formatted)
     }
  }
         
   /**
    *  Record that a track run has been started.
    *  
    *  Returns an unique ID representing the run
    *  return a string id GoalState.state to running
    */
   def startRun( trackDesc: TrackDescriptor, goalName : String, witness: Witness, startTime : DateTime) : String
   
   def startSubGoalRun ( trackDesc: TrackDescriptor, goalName : String, witness: Witness, startTime : DateTime, parentRunId: String) : String
  
   /**
    *   Mark that a Track has been completed with a certain state ( Either Succeeded or Failed )
    *   update record of id
    *   
    *   TODO : should record tracking here (such as how long it took to finish etc)
    */
   def completeRun( id : String, state : GoalState.State) : String
   
   /**
    *   Get all runs for a Track,
    *    within an optional starttime, endtime DateRange
    *    
    *     select where track = trackDesck and  startTime <= startParam <= endTime also other cases
    */
   def goalRunsForTrack(  trackDesc : TrackDescriptor , 
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun]
  
   /**
    *   Get all the runs for a specific Goal in a Track,
    *    within an optional starttime, endtime DateRange
    *    
    *    same but qualify for goalName (same as above but go deeper for each goal)
    */
   def goalRunsForGoal(  trackDesc : TrackDescriptor ,  
              goalName : String,
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun]

   /**
    *  Lookup a specific Goal runs, 
    *    given the witness specifying the run
    *    
    *  Multiple rows may be returned
    *   ( for example if a job was restarted after a   
    *      job failure)
    *      
    *      select and return to goal runs for the trackDesc
    */
   def lookupGoalRun(  trackDesc : TrackDescriptor ,  
              goalName : String,
              witness : Witness ) : Seq[GoalRun]
  

   /**
    *  get all entries in the history table
    */
   def getAllHistory() : Seq[GoalRun]
   
   /**
    * get entries for the last week
    */
   def getRecentHistory() : Seq[GoalRun]
   
  /**
   *  Lookup a specific goal run, 
   *   given the runID
   *   
   *   by runID only
   */
   def lookupGoalRun( runID : String ) : Option[GoalRun]

   
   def getParentRunId(runID: String) : Option[String]
   
   //def getChildrenRunId(runID : String) : Seq[GoalRun]
   
   
}

object TrackHistory {

}
