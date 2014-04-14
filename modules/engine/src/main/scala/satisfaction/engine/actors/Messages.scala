package com.klout
package satisfaction
package engine
package actors

case class AddProject(jarPath: String, name: String)
case class RemoveProject(name: String)
case class GetProject(name: String)
case object GetProjects

case class ProjectResult(project: Option[Track])
case class ProjectList(names: Set[String])

case object CheckEvidence
case class JobRunSuccess( val result : ExecutionResult )
case class JobRunFailed( val result : ExecutionResult )

//// Satisfy the current goal for the specified witness
case class Satisfy(forceSatisfy: Boolean)
/// Query whether the evidence already exists, and the goal 
///   has actually been completed
case class IsSatisfied(doRecursive: Boolean)
///   Abort the current execution 
case class Abort( killChildren: Boolean=true)
/// Query the current status of all witnesses 
case class WhatsYourStatus()

/// Re-run a job which has previously been marked as failure 
case class RestartJob()

///  Respond with your currrent status
case class StatusResponse(goalStatus: GoalStatus)
case class GoalSuccess(goalStatus: GoalStatus)
case class GoalFailure(goalStatus: GoalStatus)
//// Message that the actor can't handle the current request at this time ..
case class InvalidRequest(goalStatus: GoalStatus, reason : String)
