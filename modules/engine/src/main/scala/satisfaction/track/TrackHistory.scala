package com.klout
package satisfaction
package track

import org.joda.time._
import engine.actors.GoalStatus
import engine.actors.GoalState

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
         val state : GoalState.State) {
    
     /**
      *  DB Identifier for the run
      */
     var runId : String = null
  }
         
   /**
    *  Record that a track run has been started.
    *  
    *  Returns an unique ID representing the run
    */
   def startRun( trackDesc: TrackDescriptor, goalName : String, witness: Witness, startTime : DateTime) : String
  
   /**
    *   Mark that a Track has been completed with a certain state ( Either Succeeded or Failed )
    */
   def completeRun( id : String, state : GoalState.State) : String
   
   /**
    *   Get all runs for a Track,
    *    within an optional starttime, endtime DateRange
    */
   def goalRunsForTrack(  trackDesc : TrackDescriptor , 
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun]
  
   /**
    *   Get all the runs for a specific Goal in a Track,
    *    within an optional starttime, endtime DateRange
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
    */
   def lookupGoalRun(  trackDesc : TrackDescriptor ,  
              goalName : String,
              witness : Witness ) : Seq[GoalRun]
  

  /**
   *  Lookup a specific goal run, 
   *   given the runID
   */
   def lookupGoalRun( runID : String ) : Option[GoalRun]
  
}