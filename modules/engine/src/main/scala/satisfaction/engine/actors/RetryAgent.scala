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

/**
 *  RetryAgent listens for Job failures,
 *    and sends a Retry message
 */
class RetryAgent(  val retryable : Retryable, val actorToRetry : ActorRef )
      (implicit val track : Track) extends Actor with ActorLogging {
     var currentRetry: Int = 0

  def receive = {
    case GoalFailure(goalStatus) =>
      /// Only restart if the current Goal has failed...
      ///  Don't restart if it was a child depenency which died ...
      if (goalStatus.state == GoalState.Failed) {
        log.warning(s" Goal ${goalStatus.goalName} failed after ${currentRetry} retries ")
        if (currentRetry < retryable.maxRetries) {
          val waitTime = Duration.create(retryable.waitPeriod.getStandardSeconds(), TimeUnit.SECONDS)
          log.warning(s" Retrying ${goalStatus.goalName} in ${waitTime} ")
          currentRetry += 1
          //// Use akka system to schedule
          context.system.scheduler.scheduleOnce(waitTime) {
            log.warning(s" Sending RestartJob to ${goalStatus.goalName}  to $actorToRetry")
             actorToRetry ! RestartJob
          }
        } else {
          log.error(s" Received ${retryable.maxRetries} Failures after Retries; Giving up !!!")
          retryable.retryNotifier match {
            case Some(notifier) => {
              log.info(" Sending notification of final retry ")
              notifier.notify(goalStatus.witness, goalStatus.execResult)
            }
            case None => {
              log.warning(" No notifier specified; No notification will be sent")
            }
          }
        }
      }
    case GoalSuccess(goalStatus) =>
      if (currentRetry > 0) {
        log.info(s" Goal was successful after ${currentRetry} retries.")
        /// If we finally recovered, then send a message, saying that we recovered.
        retryable.retryNotifier match {
           case Some(notifier) => {
              log.info(" Sending notification of final retry ")
              notifier.notify(goalStatus.witness, goalStatus.execResult)
            }
            case None => {
              log.warning(" No notifier specified; No notification will be sent")
            }
        }
      }
      stop()
  } 

     
   def stop() = {
     context.stop( context.self)
   }

}