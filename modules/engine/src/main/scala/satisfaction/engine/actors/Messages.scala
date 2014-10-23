package satisfaction
package engine
package actors

import akka.actor.ActorRef


case class CheckEvidence( val evidencdId : String,w : Witness )
case class EvidenceCheckResult(val evidenceId : String, w: Witness, val isAlreadySatisfied : Boolean)
case class JobRunSuccess( val result : ExecutionResult )
case class JobRunFailed( val result : ExecutionResult )

//// Satisfy the current goal for the specified witness
case class Satisfy(runID: String, parentRunID: String, forceSatisfy: Boolean = false) {
   def apply() = {
     Satisfy(null,null,false)
   } 
}

object Satisfy {
   def apply() = {
      new Satisfy(null,null,false)
   } 
}

/// Query whether the evidence already exists, and the goal 
///   has actually been completed
case class IsSatisfied(doRecursive: Boolean)
///   Abort the current execution 
case class Abort( killChildren: Boolean=true)
/// Query the current status of all witnesses 
case class WhatsYourStatus()

case class AddListener(actorRef:ActorRef)

/// Re-run a job which has previously been marked as failure 
case class RestartJob(runID: String, parentRunID: String)
object RestartJob {
   def apply() = {
      new RestartJob(null,null)
   } 
  
}


///  Respond with your currrent status
case class StatusResponse(goalStatus: GoalStatus)
case class GoalSuccess(goalStatus: GoalStatus)
case class GoalFailure(goalStatus: GoalStatus)
//// Message that the actor can't handle the current request at this time ..
case class InvalidRequest(goalStatus: GoalStatus, reason : String)
