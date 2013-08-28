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
case class ReleaseActor(goal: Goal, witness: Witness)
case class GetListeners(goal: Goal, witness: Witness)

class ProverFactory extends Actor with ActorLogging {
    val actorMap: mutable.Map[String, ActorRef] = mutable.Map()
    val listenerMap: mutable.Map[String, mutable.Set[ActorRef]] = mutable.Map[String, mutable.Set[ActorRef]]()

    def receive = {
        case GetActor(goal, witness) =>
            val actorName = ProofEngine.getActorName(goal, witness)
            if (actorMap.contains(actorName)) {
                log.info(" Someone already listening to " + actorName)
                val listenerList = listenerMap.get(actorName).get
                if (!listenerList.contains(sender))
                    listenerList += sender
                sender ! actorMap.get(actorName).get
            } else {
                val actorRef = context.system.actorOf(Props(new PredicateProver(goal, witness, context.self)), actorName)
                log.info(" Created Actor with name " + actorName + " with path " + actorRef.path)
                actorMap.put(actorName, actorRef)
                val listenerList = mutable.Set[ActorRef]()
                listenerList += sender
                listenerMap.put(actorName, listenerList)
                sender ! actorRef
            }
        case ReleaseActor(goal, witness) =>
            val actorName = ProofEngine.getActorName(goal, witness)
            if (listenerMap.contains(actorName)) {
                val listenerList = listenerMap.get(actorName).get
                listenerList.remove(sender)
                if (listenerList.size == 0) {
                    listenerMap.remove(actorName)
                    val deadRef = actorMap.remove(actorName).get
                    context.stop(deadRef)
                }
            }
        case GetListeners(goal, witness) =>
            val actorName = ProofEngine.getActorName(goal, witness)
            sender ! listenerMap.get(actorName).get
        case GoalFailure(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalFailure(goalStatus))
        case GoalSuccess(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalSuccess(goalStatus))

    }

    def publishMessageToListeners(goalStatus: GoalStatus, message: Any) = {
        val actorName = ProofEngine.getActorName(goalStatus.goal, goalStatus.witness)
        log.info(" Publishing message " + message + " to all listeners of " + actorName)
        listenerMap.get(actorName) match {
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