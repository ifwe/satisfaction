package satisfaction.engine.actors

import satisfaction.notifier.Notifier
import akka.actor.Actor
import akka.actor.ActorLogging
import satisfaction.Track
import satisfaction.notifier.Notified
import satisfaction.retry.Retryable
import satisfaction.Goal
import akka.actor.ActorSystem
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import satisfaction.GoalState
import akka.actor.ActorRef
import satisfaction.track.TrackHistory
import satisfaction.GoalStatus
import satisfaction.TrackDescriptor
import satisfaction.Witness
import org.joda.time.DateTime
import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.ReceiveCounterActor
import nl.grons.metrics.scala.Counter

/**
 * JMX Agent intercepts Satisfy and GoalSuccess
 *  messages, and updates 
 *   JMX counters.
 *   
 *   JMX Agent updates counter runs
 */
class JMXAgent extends Actor with ActorLogging with satisfaction.engine.Instrumented  {
  
  val counterMap : Map[String,Counter] = Map[String,Counter]()

  /**
   *   Only count job successes and failures for now 
   *     until we rethink agent publishing ..
   *     and job lifecycle
   */
  def receive = {
    case GoalFailure(goalStatus) =>
      getCounter( goalStatus, "failure") += 1
    case GoalSuccess(goalStatus) =>
      getCounter( goalStatus, "success") += 1
    case unexpected : Any =>
    	log.warning(s" Unexpected message $unexpected in JMX Agent")
  } 

     
  def getCounter(goalStatus : GoalStatus, event : String ) : Counter =  {
      val counterName = s"${goalStatus.track.trackName}.${goalStatus.goalName}.$event" 
    
      counterMap get( counterName ) match {
        case Some(counter)  =>   counter 
        case None  => {
           val newCounter = metrics.counter( "satisfaction", counterName)
           newCounter
        }
      }
  }

}