package com.klout.satisfaction
package executor
package actors

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

import scala.concurrent.duration._

import play.api.libs.concurrent.Execution.Implicits._

case class WitnessStatus(remainingDeps: Set[(Witness, Goal)], toNotifiy: Set[ActorRef])

class GoalOwner(
    goal: Goal,
    projectParams: ParamMap) extends Actor with ActorLogging {

    var pendingWitnesses: Map[Witness, WitnessStatus] = Map.empty

    var pendingSatisfiers: Map[ActorRef, Witness] = Map.empty

    lazy val projectOwner = context.parent

    def receive = {
        case GoalSatisfied =>
            val satisfier = context.sender
            pendingSatisfiers get satisfier match {
                case Some(witness) =>
                    pendingWitnesses get witness foreach {
                        case WitnessStatus(_, toNotify) =>
                            toNotify foreach { interestedParty =>
                                interestedParty ! IAmDone(witness, goal)
                            }
                    }
                    pendingWitnesses -= witness
                    pendingSatisfiers -= satisfier
                case _ =>
            }
            context.stop(satisfier)

        case AreYouDone(witness) =>
            println(" Goal Owner Received Message  Are you done " + witness)
            ///val params = getParams(witness)
            if (goal.evidence forall (_.exists(witness))) {
                sender ! IAmDone(witness, goal)
            } else {
                println(" I am not all done ")
                val witnessStatus = pendingWitnesses get witness match {
                    case None =>
                        val initalDependencies = goal.dependencies map {
                            case (witnessFunction, goal) =>
                                witnessFunction(witness) -> goal
                        }
                        val initialToNotify = Set(sender)
                        val newStatus = WitnessStatus(initalDependencies, initialToNotify)
                        pendingWitnesses += witness -> newStatus
                        newStatus

                    case Some(witnessStatus) =>
                        val newStatus = witnessStatus copy (toNotifiy = witnessStatus.toNotifiy + sender)
                        pendingWitnesses += witness -> newStatus
                        newStatus
                }

                witnessStatus.remainingDeps foreach {
                    case (dependentWitness, goal) =>
                        context.actorSelection(goal.uniqueId) ! AreYouDone(dependentWitness)
                }

            }

        case IAmDone(witness, goal) =>
            pendingWitnesses foreach {
                case (pendingWitness, currentStatus) =>
                    val remainingDeps = currentStatus.remainingDeps
                    val recentlyCompletedDep = witness -> goal
                    if (remainingDeps contains recentlyCompletedDep) {
                        val newDeps = remainingDeps - recentlyCompletedDep
                        val newStatus = currentStatus copy (remainingDeps = newDeps)
                        pendingWitnesses += pendingWitness -> newStatus
                        if (newDeps.isEmpty) {
                            satisfy(pendingWitness)
                        }
                    }
            }
    }

    def satisfy(witness: Witness) {
        val params = getParams(witness)
        val satisfierActor = goal.satisfier match {
            case Some(satisfier) => context.actorOf(Props(new GoalSatisfier(satisfier, params)))
            case None            => context.actorOf(Props(new DefaultGoalSatisfier(goal.evidence, params, witness)))
        }
        pendingSatisfiers += satisfierActor -> witness
        satisfierActor ! SatisfyGoal
    }

    def getParams(witness: Witness): ParamMap =
        ///projectParams ++ witness.params ++ (goal.overrides getOrElse ParamOverrides.empty)
        witness.params ++ (goal.overrides getOrElse ParamOverrides.empty)
}
