package satisfaction
package engine
package actors

import akka.actor.Actor
import akka.actor._
import akka.pattern._
import akka.actor.ActorLogging
import scala.collection._
import scala.concurrent.Await
import satisfaction._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import akka.util.Timeout
import satisfaction.track.TrackFactory
import satisfaction.track.TrackHistory
import satisfaction.notifier.Notified
import satisfaction.retry.Retryable
import org.joda.time.DateTime


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

class ProverFactory( trackHistoryOpt : Option[TrackHistory] = None) extends Actor with ActorLogging {
    ///val actorMap: mutable.Map[Tuple2[Goal, Witness], ActorRef] = mutable.Map()
    ///val listenerMap: mutable.Map[Tuple2[Goal, Witness], mutable.Set[ActorRef]] = mutable.Map[Tuple2[Goal, Witness], mutable.Set[ActorRef]]()
    private val _actorMap: mutable.Map[String, ActorRef] = mutable.Map()
    private val _referenceMap: mutable.Map[String, mutable.Set[ActorRef]] = mutable.Map[String, mutable.Set[ActorRef]]()
    
    val trackHistory : TrackHistory = trackHistoryOpt.getOrElse(null)
    
    
    implicit val ec = ExecutionContext.Implicits.global

    /**
     *  Validate that the witness contains all the variables needed for the goal
     *   XXX move to utility
     */
    def checkVariables( goal : Goal, wit : Witness ) : Boolean = {
       goal.variables.foreach( v => {
          if(!wit.contains(v)) {
              log.warning(s" Witness $wit doesn't have variable ${v.name} to satisfy goal ${goal.name} ")
              return false
          }
       })
       
       true
    }
    
    
    def pimpMyActor( actor : ActorRef, track : Track, goal : Goal, witness : Witness)  = {
       //// want to be able to add "plugins" around the prover actor         
      //// So that we can do more complex logic around it
       track match {
           case  notified : Notified => {
             implicit val track : Track = goal.track
              log.info(s" Setting up notification for goal ${goal.name} and witness  $witness" )
              val notifierAgent : ActorRef = context.system.actorOf(Props(classOf[NotificationAgent], notified),
                  "NOTIFIER_" + actor.path.name)
              actor ! AddListener( notifierAgent)
           }
           case _  => {
             log.info( "No Notification setup for goal ${goalName} ")
           }
      }
      track match {
        case retryable : Retryable => {
          if( retryable.shouldRetry( goal)) {
             log.info(s" Goal ${goal.name} is retryable ; creating RetryAgent to listen to status ")
             val retryAgent : ActorRef = context.system.actorOf(Props(new RetryAgent(retryable,actor )(track)))
             actor ! AddListener( retryAgent)
          } else {
            log.warning(" Retryable says we should not retry goal ${goal.name}; Not creating RetryAgent ")
          }
        }
        case _  => {
          log.info( "No Retry setup for goal ${goalName} ")
        }
      }
        
    }
    
    
    
    /**
     *  Hold onto an ActorReference for performing various activities
     *   ( i.e getStatus, Satisfy, Abort , etc ... )
     */
    def acquireReference( goalName : String, witness :  Witness, actorRef: ActorRef) = {
        val actorTuple = (goalName, witness)
        val actorTupleName = ProofEngine.getActorName(goalName, witness)
        if( !sender.path.toString().contains("temp")) {
        val checkList = _referenceMap.get( actorTupleName)
        checkList match {
          case Some(list) => {
             if( !list.contains( sender))  {
                 list.add(sender) 
             }
          } 
          case None => {
             val newList = mutable.Set[ActorRef]()
             newList.add( sender)
             _referenceMap.put(actorTupleName, newList)
          }
        }
       }
    }
    
    
    def createProverActor(track : Track, goal : Goal, witness : Witness) : ActorRef = {
       val actorTupleName = ProofEngine.getActorName(goal.name, witness)
       val actorRef = context.system.actorOf(Props(classOf[PredicateProver],track, goal, witness, context.self),
            actorTupleName)
       acquireReference( goal.name, witness, sender)
                    
       /// XXX build actor pimping framework
       if( trackHistory != null) {
          val historyRef = context.system.actorOf(Props(classOf[HistoryAgent], actorRef,  track.descriptor, goal.name, witness,trackHistory),
             "History_" + actorTupleName)
          _actorMap.put(actorTupleName,historyRef)
           actorRef ! AddListener( historyRef)
                

          pimpMyActor( historyRef, track, goal, witness)
          historyRef      
       } else {
          _actorMap.put(actorTupleName,actorRef)
          pimpMyActor( actorRef, track, goal, witness)
          actorRef 
       }
    }
    
    def containsActor( actorTupleName : String ) : Boolean = {
       _actorMap.contains(actorTupleName) 
    }

    def receive = {
        case GetActor(track, goal, witnessArg) =>
            val witness = witnessArg.filter( goal.variables.toSet)
            
            log.info(s"Getting ProverActor for goal ${goal.name} and witness $witness vs $witnessArg ; Goal Variables are ${goal.variables}")
            checkVariables( goal, witness)
            val actorTuple: Tuple2[Goal, Witness] = (goal, witness)
            val actorTupleName = ProofEngine.getActorName(goal.name, witness)
            println("Before check for Tuple " + ProofEngine.getActorName(goal.name, witness))
            if (containsActor(actorTupleName)) {
                sender ! _actorMap.get(actorTupleName).get
            } else {
                val actorRef = createProverActor( track, goal, witness)
               
                sender ! actorRef
            }
        case ReleaseActor(goalName, witnessArg) =>
            //val witness = witnessArg.filter( goal.variables.toSet)
            val witness = witnessArg
            log.info( "Received a Release Actor message for goal " + goalName + " witness " + witness + " Sender is " + sender )
            val actorTuple = (goalName, witness)
            val actorTupleName = ProofEngine.getActorName(goalName, witness)
            if (_referenceMap.contains(actorTupleName)) {
                val listenerList = _referenceMap.get(actorTupleName).get
                listenerList.remove(sender)
                if (listenerList.size == 0) {
                    _referenceMap.remove(actorTupleName)
                    val deadRef = _actorMap.remove(actorTupleName).get
                    log.info( s"Stopping actor $actorTupleName with no more listener " )
                    //// Tell him to stop all h
                    deadRef ! ReleaseActor(goalName,witness)
                    context.system.scheduler.scheduleOnce( 10 seconds, self, new KillActor(goalName,witness) )
                } else {
                  log.info(s" ${listenerList.size} actors remaining !!!  ${listenerList.mkString(";")}" )
                }
            }
        case GoalFailure(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalFailure(goalStatus))
        case GoalSuccess(goalStatus) =>
            publishMessageToListeners(goalStatus, new GoalSuccess(goalStatus))
            //// Schedule a message to release this actor after a while
         context.system.scheduler.scheduleOnce( 30 seconds, self, new KillActor( goalStatus.goalName, goalStatus.witness) )
        case KillActor( goalName,witness) =>
            log.info(s" Killing Actor for goal ${goalName} $witness") 
            val actorTupleName = ProofEngine.getActorName(goalName, witness)
            _actorMap .remove( actorTupleName ) match {
              case Some(actorRef : ActorRef) =>
                context.stop( actorRef)
              case None =>
                log.warning(s"Unable to find actor $actorTupleName to be killed")
            }
            _referenceMap.remove(actorTupleName)

        case GetActiveActors =>
            val activeActors = _actorMap.values.toSet
            sender ! activeActors

    }

    def publishMessageToListeners(goalStatus: GoalStatus, message: Any) = {
       log.info(s" Publish $message To Listeneners in ProverFactory ??? ")
      /**
        val actorTuple = (goalStatus.goalName, goalStatus.witness)
        val actorTupleName = ProofEngine.getActorName(goalStatus.goalName, goalStatus.witness)
        log.info(" Publishing message " + message + " to all listeners of " + actorTuple)
        _referenceMap.get(actorTupleName) match {
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
        * 
        */
    }

}
object ProverFactory {

    implicit val timeout = Timeout( 3000 seconds )
    
    def acquireProver(proverFactory: ActorRef, track : Track, goal: Goal, witness: Witness): ActorRef = {
        val f = proverFactory ?  GetActor( track,goal,witness)
        Await.result(f, timeout.duration).asInstanceOf[ActorRef]
    }


    def releaseProver(proverFactory: ActorRef, goal: Goal, witness: Witness) = {
        proverFactory ! ReleaseActor(goal.name,witness)
    }

}