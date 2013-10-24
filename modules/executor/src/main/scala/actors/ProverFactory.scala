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
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext


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
case class KillActor( goal : Goal, witness : Witness)
case class GetListeners(goal: Goal, witness: Witness)

class ProverFactory extends Actor with ActorLogging {
    ///val actorMap: mutable.Map[Tuple2[Goal, Witness], ActorRef] = mutable.Map()
    ///val listenerMap: mutable.Map[Tuple2[Goal, Witness], mutable.Set[ActorRef]] = mutable.Map[Tuple2[Goal, Witness], mutable.Set[ActorRef]]()
    val actorMap: mutable.Map[String, ActorRef] = mutable.Map()
    val listenerMap: mutable.Map[String, mutable.Set[ActorRef]] = mutable.Map[String, mutable.Set[ActorRef]]()
    
    implicit val ec = ExecutionContext.Implicits.global

    /**
     *  Validate that the witness contains all the variables needed for the goal
     *   XXX move to utility
     */
    def checkVariables( goal : Goal, wit : Witness ) : Boolean = {
       goal.variables.foreach( v => {
          if(!wit.substitution.contains(v)) {
              log.warning(s" Witness $wit doesn't have variable ${v.name} to satisfy goal ${goal.name} ")
              return false
          }
       })
       
       true
    }
    
    def receive = {
        case GetActor(goal, witness) =>
            ///val actorTuple = ProofEngine.getactorTuple(goal, witness)
            log.info(s"Getting ProverActor for goal $goal.name and witness $witness ")
            checkVariables( goal, witness)
            val actorTuple: Tuple2[Goal, Witness] = (goal, witness)
            val actorTupleName = ProofEngine.getActorName(goal, witness)
            println("Before check for Tuple map is  " + actorMap)
            println("Before check for Tuple " + ProofEngine.getActorName(goal, witness))
            if (actorMap.contains(actorTupleName)) {
                val listenerList = listenerMap.get(actorTupleName).get
                if (!listenerList.contains(sender))
                    listenerList += sender
                sender ! actorMap.get(actorTupleName).get
            } else {
                val actorRef = context.system.actorOf(Props(new PredicateProver(goal, witness, context.self)),
                    ProofEngine.getActorName(goal, witness))
                actorMap.put(actorTupleName, actorRef)
                val listenerList = mutable.Set[ActorRef]()
                listenerList += sender
                listenerMap.put(actorTupleName, listenerList)
                sender ! actorRef
            }
        case ReleaseActor(goal, witness) =>
            log.info( "Received a Release Actor message for goal " + goal.name + " witness " + witness )
            val actorTuple = (goal, witness)
            val actorTupleName = ProofEngine.getActorName(goal, witness)
            if (listenerMap.contains(actorTupleName)) {
                val listenerList = listenerMap.get(actorTupleName).get
                listenerList.remove(sender)
                if (listenerList.size == 0) {
                    listenerMap.remove(actorTupleName)
                    val deadRef = actorMap.remove(actorTupleName).get
                    log.info( s"Stopping actor $actorTupleName with no more listener " )
                    context.stop(deadRef)
                }
            }
        case GetListeners(goal, witness) =>
            val actorTuple = (goal, witness)
            val actorTupleName = ProofEngine.getActorName(goal, witness)
            sender ! listenerMap.get(actorTupleName).get
        case GoalFailure(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalFailure(goalStatus))
        case GoalSuccess(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalSuccess(goalStatus))
            //// Schedule a message to release this actor after a while
            system.scheduler.scheduleOnce( 30 seconds, self, new KillActor( goalStatus.goal, goalStatus.witness) )
        case KillActor( goal,witness) =>
            log.info(s" Killing Actor for goal ${goal.name} $witness") 
            val actorTupleName = ProofEngine.getActorName(goal, witness)
            actorMap .remove( actorTupleName ) match {
              case Some(actorRef : ActorRef) =>
                context.stop( actorRef)
              case None =>
                log.warning(s"Unable to find actor $actorTupleName to be killed")
            }
        case GetActiveActors =>
            val activeActors = actorMap.values.toSet
            println("Number of active actors is " + activeActors.size + " Map size  " + actorMap.size)
            sender ! activeActors

    }

    def publishMessageToListeners(goalStatus: GoalStatus, message: Any) = {
        val actorTuple = (goalStatus.goal, goalStatus.witness)
        val actorTupleName = ProofEngine.getActorName(goalStatus.goal, goalStatus.witness)
        log.info(" Publishing message " + message + " to all listeners of " + actorTuple)
        listenerMap.get(actorTupleName) match {
            case Some(listenerList) =>
                listenerList.foreach { listenRef =>
                    log.info(" sending to listener " + listenRef)
                    ///val actorRef = context.system.actorFor(actorPath)
                    ///actorRef ! message
                    listenRef ! message
                }
            case None =>
              log.warning(s" No listener list found for actor tuple $actorTupleName ")
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