package satisfaction.engine.actors

import satisfaction.notifier.Notifier
import akka.actor.Actor
import akka.actor.ActorLogging
import satisfaction.Track
import satisfaction.notifier.Notified
import satisfaction.retry.Retryable
import satisfaction.Goal
import akka.actor.ActorSystem
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import satisfaction.GoalState
import akka.actor.ActorRef
import satisfaction.track.TrackHistory
import satisfaction.GoalStatus
import satisfaction.TrackDescriptor
import satisfaction.Witness
import org.joda.time.DateTime

/**
 * HistoryAgent intercepts Satisfy and 
 */
class HistoryAgent( val forwardActor : ActorRef, 
          val trackDesc: TrackDescriptor, 
          val goalName : String, 
          val witness : Witness, 
          val trackHistory : TrackHistory ) /// XXX Need to be serializable ? )
      extends Actor with ActorLogging {

  var runID : String  = null ;
  def receive = {
    //// Satisfy message will have RunID as null
    //// 
    case Satisfy(nullRunID,parentRunID,force) =>
      this.runID = generateRunID(parentRunID)
      log.info(s"HISTORY - Satisfy -- Generating runID $runID for parentRun $parentRunID")
      forwardActor forward Satisfy(runID=this.runID,parentRunID=parentRunID,forceSatisfy=force )
      
    case RestartJob(nullRunID,parentRunID) =>
      this.runID = generateRunID(parentRunID)
      log.info(s"HISTORY - Restart Generating runID $runID for parentRun $parentRunID")
      forwardActor forward RestartJob(runID=this.runID,parentRunID=parentRunID )
    case GoalFailure(goalStatus) =>
      log.info(s"HistoryAgent received status FAILURE $runID" )
      completeRun(goalStatus)
    case GoalSuccess(goalStatus) =>
      log.info(s"HistoryAgent received status SUCCESS $runID" )
      completeRun(goalStatus)
      
    case unexpected : Any =>
      log.info(s"HISTORY  Forwarding message $unexpected ")
      forwardActor forward unexpected
  } 
  
  
  def generateRunID( parentRunID : String ) : String = {
    if(parentRunID == null) {
        log.info(s" Track ${trackDesc} History start Run  ${goalName} ${witness} " )
        trackHistory.startRun(trackDesc, goalName, witness, DateTime.now)
    } else {
       log.info(s" Track ${trackDesc} History start SuboGoal Run  ${goalName} ${witness} ; Parent is $parentRunID" )
       trackHistory.startSubGoalRun(trackDesc, goalName, witness, DateTime.now, parentRunID)
    }
    
  }

  def completeRun( goalStatus : GoalStatus ) = {
    if( runID != null)
       trackHistory.completeRun( runID, goalStatus.state)
  }
     
   override def postStop() = {
      //// Kill the forward actor ..
     context.system.stop( forwardActor)
   }

}