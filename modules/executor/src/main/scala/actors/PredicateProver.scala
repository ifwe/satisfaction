package com.klout.satisfaction
package executor
package actors

import com.klout.satisfaction.executor.actors.GoalStatus._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.Future

//// Satisfy the current goal for the specified witness
case class Satisfy(forceSatisfy: Boolean)
/// Query whether the evidence already exists, and the goal 
///   has actually been completed
case class IsSatisfied(doRecursive: Boolean)
///   Abort the current execution 
case class Abort()
/// Query the current status of all witnesses 
case class WhatsYourStatus()

///  Respond with your currrent status
case class StatusResponse(goalStatus: GoalStatus)
case class JobFailed(goalStatus: GoalStatus)
case class Success(goalStatus: GoalStatus)
case class Failure(goalStatus: GoalStatus)
//// Message that the actor can't handle the current request at this time ..
case class InvalidRequest(goalStatus: GoalStatus)

/**
 *  Actor who's responsibility is to satisfy a goal
 *
 *
 *  XXX TODO Use event bus to handle multiple listeners for events
 *  XXX Handle case to see if fully satisfied ( dependencies are satisfied)
 *  XXX  and cases where we want to force completion
 *
 *  XXX Handle ActorNaming -- Why is namespace different
 */
class PredicateProver(val goal: Goal, val witness: Witness) extends Actor with ActorLogging {

    val dependencies: scala.collection.mutable.Map[String, ActorRef] = scala.collection.mutable.Map[String, ActorRef]()
    var jobRunner: ActorRef = null
    val status: GoalStatus = new GoalStatus(goal, witness)
    var asker: ActorRef = null

    def receive = {
        /// Messages which can be sent from parents 

        case Satisfy =>
            /// Check to see if 
            if (goal.evidence.forall(e => e.exists(witness))) {
                status.state = GoalState.AlreadySatisfied
                sender ! Success(status)
            } else {
                asker = sender
                if (status.state != GoalState.Unstarted) {
                    sender ! InvalidRequest(status)
                }
                /// Go through our dependencies, and ask them to
                /// satify
                if (dependencies.size > 0) {
                    dependencies.foreach {
                        case (pred, actor) =>
                            actor ! Satisfy
                    }
                    status.state = GoalState.WaitingOnDependencies
                } else {
                    //// Send yourself an empty message 

                    ////system.actorFor(goal.getPredicateString(witness)) ! Success
                    runLocalJob()
                    ///context.self ! Success

                }
            }

        case WhatsYourStatus =>
            //// Do a blocking call to just return  

            val currentStatus = new GoalStatus(goal, witness)
            currentStatus.state = status.state

            /// Go through and ask all our 
            val futureSet = dependencies.map {
                case (pred, actorRef) =>
                    actorRef ask WhatsYourStatus
            }
            futureSet.foreach(f => {
                currentStatus.addChildStatus(f.asInstanceOf[StatusResponse].goalStatus)
            })
            sender ! StatusResponse(currentStatus)

        case Abort     =>

        /// Messages which can be sent from children
        case JobFailed =>
        case Failure(failedStatus) =>
            //// 
            println("Failure in our Chidren")
            status.addChildStatus(failedStatus)
            status.state = GoalState.DepFailed
            asker ! Failure(status)
        //// Add a flag to see if we want to 
        //// abort sibling jobs which may be running 
        case Success(depStatus) =>
            if (depStatus != null)
                status.addChildStatus(depStatus)
            //// Determine if 
            if (status.dependencyStatus.size == dependencies.size) {
                runLocalJob()
            }
        //// XXX Refactor names 
        case GoalSatisfied =>
            log.info(" Received Goal Satisfied, send to our parent  ")
            status.state = GoalState.Success
            ///context.parent ! Success(status)
            asker ! Success(status)
        case GoalFailed =>
            log.info(" Received Goal Failed, send to our parent  ")
            status.state = GoalState.Failed
            ///context.parent ! Success(status)
            asker ! Failure(status)

        case InvalidRequest =>
    }

    def runLocalJob() {
        status.state = GoalState.SatifyingSelf
        goal.satisfier match {
            case Some(satisfier) =>
                val jobRunActor = Props(new GoalSatisfier(satisfier, getParamMap))
                this.jobRunner = context.system.actorOf((jobRunActor), "Satisfier_" + goal.name)
                jobRunner ! Satisfy
            case None =>
                //// XXX Refactor names 
                val jobRunActor = Props(new DefaultGoalSatisfier(goal.evidence, getParamMap, witness))
                this.jobRunner = context.system.actorOf(jobRunActor)
                jobRunner ! Satisfy
        }
    }

    def getParamMap: ParamMap = {
        witness.params
    }

    override def preStart() = {

        //// Create actors for all the sub actors, recursively
        goal.dependencies.foreach{
            case (wmap: (Witness => Witness), subGoal: Goal) =>
                //// Generate different witness, based on the mapping function
                val newWitness = wmap(witness)
                val depPredicate = subGoal.getPredicateString(newWitness)
                ///val checkActor = context.system.actorFor(depPredicate)
                ///if (checkActor == null) {
                if (true) {
                    val depProps = Props(new PredicateProver(subGoal, newWitness))
                    //// XXX Figure out actor name 
                    ///val depProverRef = context.system.actorOf(depProps, ProofEngine.getActorName(subGoal, newWitness))
                    val actorPath = context.self.path / ProofEngine.getActorName(subGoal, newWitness)
                    log.info(" Creating child actor with path " + actorPath + " :: " + actorPath.name)
                    val depProverRef = context.system.actorOf(depProps, actorPath.name)
                    dependencies += (depPredicate -> depProverRef)
                } else {
                    ///dependencies += (depPredicate -> checkActor)
                    false
                }
        }

    }

}