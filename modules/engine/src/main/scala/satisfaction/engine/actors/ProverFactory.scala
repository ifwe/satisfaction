package com.klout
package satisfaction
package engine
package actors

import akka.actor.Actor
import akka.actor._
import akka.pattern._
import akka.actor.ActorLogging
import scala.collection._
import scala.concurrent.Await
//import com.klout.satisfaction.Goal
import satisfaction._
//import com.klout.satisfaction.Witness
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import akka.util.Timeout


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

case class GetActor(track : Track, goal: Goal, witness: Witness)
case class GetActiveActors()
case class ReleaseActor(goalName : String, witness: Witness)
case class KillActor( goalName : String, witness : Witness)
case class GetListeners(goalName: String, witness: Witness)

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
        case GetActor(track, goal, witnessArg) =>
            val witness = witnessArg.filter( goal.variables.toSet)
            
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
                val actorRef = context.system.actorOf(Props(new PredicateProver(track, goal, witness, context.self)),
                    ProofEngine.getActorName(goal, witness))
                actorMap.put(actorTupleName, actorRef)
                val listenerList = mutable.Set[ActorRef]()
                listenerList += sender
                listenerMap.put(actorTupleName, listenerList)
                sender ! actorRef
            }
        case ReleaseActor(goalName, witnessArg) =>
            //val witness = witnessArg.filter( goal.variables.toSet)
            val witness = witnessArg
            log.info( "Received a Release Actor message for goal " + goalName + " witness " + witness )
            val actorTuple = (goalName, witness)
            val actorTupleName = ProofEngine.getActorName(goalName, witness)
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
        case GetListeners(goal, witnessArg) =>
            //// XXX Do we need to filter witness variables somehow ???
            ////val witness = witnessArg.filter( goal.variables.toSet)
            val witness = witnessArg
            val actorTuple = (goal, witness)
            val actorTupleName = ProofEngine.getActorName(goal, witness)
            sender ! listenerMap.get(actorTupleName).get
        case GoalFailure(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalFailure(goalStatus))
        case GoalSuccess(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalSuccess(goalStatus))
            //// Schedule a message to release this actor after a while
            context.system.scheduler.scheduleOnce( 30 seconds, self, new KillActor( goalStatus.goalName, goalStatus.witness) )
        case KillActor( goalName,witness) =>
            log.info(s" Killing Actor for goal ${goalName} $witness") 
            val actorTupleName = ProofEngine.getActorName(goalName, witness)
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
        val actorTuple = (goalStatus.goalName, goalStatus.witness)
        val actorTupleName = ProofEngine.getActorName(goalStatus.goalName, goalStatus.witness)
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

    implicit val timeout = Timeout( 30 seconds )
    
    def getProver(proverFactory: ActorRef, track : Track, goal: Goal, witness: Witness): ActorRef = {
        val f = proverFactory ? GetActor(track, goal, witness)
        Await.result(f, timeout.duration).asInstanceOf[ActorRef]
    }

    def releaseProver(proverFactory: ActorRef, goal: Goal, witness: Witness) = {
        proverFactory ! ReleaseActor(goal.name, witness)
    }

}