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
class JMXAgent( val forwardActor : ActorRef, 
          val trackDesc: TrackDescriptor, 
          val goalName : String, 
          val witness : Witness )
      extends Actor with ActorLogging with satisfaction.engine.Instrumented  {
  
  val jmxCounterStarted : Counter  = metrics.counter( "satisfaction", s"${trackDesc.trackName}.$goalName.started")
  val jmxCounterSuccess : Counter  = metrics.counter( "satisfaction", s"${trackDesc.trackName}.$goalName.success")
  val jmxCounterFailed : Counter  = metrics.counter( "satisfaction", s"${trackDesc.trackName}.$goalName.failed")
  val jmxCounterUnexpected : Counter  = metrics.counter( "satisfaction", s"${trackDesc.trackName}.$goalName.unexpected")

  def receive = {
    case Satisfy(runID,parentRunID,force) =>
      jmxCounterStarted += 1
      forwardActor forward Satisfy(runID,parentRunID,force)
      
    case RestartJob(runID,parentRunID) =>
      jmxCounterStarted += 1
      forwardActor forward RestartJob(runID=runID,parentRunID=parentRunID )
    case GoalFailure(goalStatus) =>
      jmxCounterFailed += 1
    case GoalSuccess(goalStatus) =>
      jmxCounterSuccess += 1
    case unexpected : Any =>
      jmxCounterUnexpected += 1
      forwardActor forward unexpected
  } 

     
   override def postStop() = {
      //// Kill the forward actor ..
     context.system.stop( forwardActor)
   }

}