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
import scala.collection._
import scala.concurrent.duration._

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
case class GoalSuccess(goalStatus: GoalStatus)
case class GoalFailure(goalStatus: GoalStatus)
//// Message that the actor can't handle the current request at this time ..
case class InvalidRequest(goalStatus: GoalStatus)

/**
 *  Actor who's responsibility is to satisfy a goal
 *
 *
 *  XXX Handle case to see if fully satisfied ( dependencies are satisfied)
 *  XXX  and cases where we want to force completion
 *
 */
class PredicateProver(val goal: Goal, val witness: Witness, val proverFactory: ActorRef) extends Actor with ActorLogging {

    val dependencies: mutable.Map[String, ActorRef] = scala.collection.mutable.Map[String, ActorRef]()
    var jobRunner: ActorRef = null
    val status: GoalStatus = new GoalStatus(goal, witness)
    var listenerList: Set[ActorRef] = mutable.Set[ActorRef]()

    def receive = {
        /// Messages which can be sent from parents 

        case Satisfy =>
            /// Check to see if 
            if (goal.evidence != null &&
                goal.evidence.size != 0 &&
                goal.evidence.forall(e => e.exists(witness))) {
                println(" Check Already satisfied ?? ")
                status.state = GoalState.AlreadySatisfied
                sender ! GoalSuccess(status)
            } else {
                listenerList += sender
                if (status.state != GoalState.Unstarted) {
                    sender ! InvalidRequest(status)
                } else {
                    /// Go through our dependencies, and ask them to
                    /// satify
                    if (dependencies.size > 0) {
                        dependencies.foreach {
                            case (pred, actor) =>
                                actor ! Satisfy
                        }
                        status.state = GoalState.WaitingOnDependencies
                    } else {
                        runLocalJob()
                    }
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

        case Abort =>

        /// Messages which can be sent from children
        case GoalFailure(failedStatus) =>
            //// 
            println("Failure in our Chidren")
            status.addChildStatus(failedStatus)
            status.state = GoalState.DepFailed
            publishFailure
        //// Add a flag to see if we want to 
        //// abort sibling jobs which may be running 
        case GoalSuccess(depStatus) =>
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
            publishSuccess
        case GoalFailed =>
            log.info(" Received Goal Failed, send to our parent  ")
            status.state = GoalState.Failed
            publishFailure

        case InvalidRequest =>
    }

    def publishSuccess = {
        ////proverFactory ! Success(status)

        ///val f = proverFactory ? GetListeners(goal, witness)
        ///val response = Await.result(f, Duration(30, SECONDS))
        ///response.asInstanceOf[Set[ActorRef]].foreach { lref =>
        ///lref ! Success(status)
        ///}
        listenerList.foreach{ actor: ActorRef =>
            actor ! GoalSuccess(status)
        }
    }
    def publishFailure = {
        listenerList.foreach{ actor: ActorRef =>
            actor ! GoalFailure(status)
        }
    }

    def runLocalJob() {
        if (status.state != GoalState.SatifyingSelf) {
            status.state = GoalState.SatifyingSelf
            goal.satisfier match {
                case Some(satisfier) =>
                    val jobRunActor = Props(new JobRunner(satisfier, getSubstitution))
                    this.jobRunner = context.system.actorOf((jobRunActor), "Satisfier_" + ProofEngine.getActorName(goal, witness))
                    jobRunner ! Satisfy
                case None =>
                    //// XXX Refactor names 
                    val jobRunActor = Props(new DefaultGoalSatisfier(
                        immutable.Set(goal.evidence.toSeq: _*), getSubstitution, witness))
                    this.jobRunner = context.system.actorOf(jobRunActor)
                    jobRunner ! Satisfy
            }
        }
    }

    def getSubstitution: Substitution = {
        witness.substitution
    }

    override def preStart() = {

        //// Create actors for all the sub actors, recursively
        if (goal.dependencies != null) {
            goal.dependencies.foreach{
                case (wmap: (Witness => Witness), subGoal: Goal) =>
                    //// Generate different witness, based on the mapping function
                    val newWitness = wmap(witness)
                    val depPredicate = subGoal.getPredicateString(newWitness)
                    val depProverRef = ProverFactory.getProver(proverFactory, subGoal, newWitness)
                    dependencies += (depPredicate -> depProverRef)
            }
        }

    }

}