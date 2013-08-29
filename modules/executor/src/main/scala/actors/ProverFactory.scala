package com.klout.satisfaction
package executor
package actors

import akka.actor.Actor
import akka.actor._
import akka.pattern._
import akka.actor.ActorLogging
import scala.collection._
import scala.concurrent.Await
import com.klout.satisfaction.Goal
import com.klout.satisfaction.Witness

/**
 *  Actor responsible for creating PredicateProver actors
 *   Because of actorFor/actorOf semantics
 *
 *   There might be some better way to do this with Akka,
 *     but for now, keep an actor in charge of creating
 *     new instances of Predicates
 *
 *
 *     Also Overload as a simple message bus
 *
 */

case class GetActor(goal: Goal, witness: Witness)
case class GetActiveActors
case class ReleaseActor(goal: Goal, witness: Witness)
case class GetListeners(goal: Goal, witness: Witness)

class ProverFactory extends Actor with ActorLogging {
    val actorMap: mutable.Map[Tuple2[Goal, Witness], ActorRef] = mutable.Map()
    val listenerMap: mutable.Map[Tuple2[Goal, Witness], mutable.Set[ActorRef]] = mutable.Map[Tuple2[Goal, Witness], mutable.Set[ActorRef]]()

    def receive = {
        case GetActor(goal, witness) =>
            ///val actorTuple = ProofEngine.getactorTuple(goal, witness)
            val actorTuple: Tuple2[Goal, Witness] = (goal, witness)
            if (actorMap.contains(actorTuple)) {
                val listenerList = listenerMap.get(actorTuple).get
                if (!listenerList.contains(sender))
                    listenerList += sender
                sender ! actorMap.get(actorTuple).get
            } else {
                val actorRef = context.system.actorOf(Props(new PredicateProver(goal, witness, context.self)),
                    ProofEngine.getActorName(goal, witness))
                actorMap.put(actorTuple, actorRef)
                val listenerList = mutable.Set[ActorRef]()
                listenerList += sender
                listenerMap.put(actorTuple, listenerList)
                sender ! actorRef
            }
        case ReleaseActor(goal, witness) =>
            val actorTuple = (goal, witness)
            if (listenerMap.contains(actorTuple)) {
                val listenerList = listenerMap.get(actorTuple).get
                listenerList.remove(sender)
                if (listenerList.size == 0) {
                    listenerMap.remove(actorTuple)
                    val deadRef = actorMap.remove(actorTuple).get
                    context.stop(deadRef)
                }
            }
        case GetListeners(goal, witness) =>
            val actorTuple = (goal, witness)
            sender ! listenerMap.get(actorTuple).get
        case GoalFailure(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalFailure(goalStatus))
        case GoalSuccess(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalSuccess(goalStatus))
        case GetActiveActors =>
            val activeActors = actorMap.values.toSet
            println("Number of active actors is " + activeActors.size + " Map size  " + actorMap.size)
            sender ! activeActors

    }

    def publishMessageToListeners(goalStatus: GoalStatus, message: Any) = {
        val actorTuple = (goalStatus.goal, goalStatus.witness)
        log.info(" Publishing message " + message + " to all listeners of " + actorTuple)
        listenerMap.get(actorTuple) match {
            case Some(listenerList) =>
                listenerList.foreach { listenRef =>
                    log.info(" sending to listener " + listenRef)
                    ///val actorRef = context.system.actorFor(actorPath)
                    ///actorRef ! message
                    listenRef ! message
                }
            case None =>
        }
    }

}
object ProverFactory {

    def getProver(proverFactory: ActorRef, goal: Goal, witness: Witness): ActorRef = {
        val f = proverFactory ? GetActor(goal, witness)
        Await.result(f, timeout.duration).asInstanceOf[ActorRef]
    }

    def releaseProver(proverFactory: ActorRef, goal: Goal, witness: Witness) = {
        proverFactory ! ReleaseActor(goal, witness)
    }

}