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

/**
 *  RetryAgent listens for Job failures,
 *    and sends a Retry message
 */
class RetryAgent(  val retryable : Retryable )
      (implicit val track : Track) extends Actor with ActorLogging {
 
      
     def receive = {
        case GoalFailure(goalStatus) =>
          log.warning(s" Goal ${goalStatus.goalName} failed after ${retryable.currentRetry} retries ")
          if( retryable.currentRetry < retryable.maxRetries) {
            val waitTime = Duration.create( retryable.waitPeriod.getStandardSeconds(), TimeUnit.SECONDS )
            log.warning(s" Retrying ${goalStatus.goalName} in ${waitTime} ") 
            retryable.currentRetry += 1
            //// Use akka system to schedule
            context.system.scheduler.scheduleOnce( waitTime ) {
               log.warning(s" Sending RestartJob to ${goalStatus.goalName} ") 
               sender ! RestartJob 
            }
          } else {
            log.error(s" Received ${retryable.maxRetries} Failures after Retries; Giving up !!!")
            retryable.notifier match {
              case Some(notifier) => {
                log.info(" Sending notification of final retry ")
            	notifier.notify( goalStatus.witness, goalStatus.execResult )
              }
              case None => {
                log.warning(" No notifier specified; No notification will be sent")
              }
            }
          }
        case GoalSuccess(goalStatus) =>
          if(retryable.currentRetry > 0 ) {
             log.info(s" Goal was successful after ${retryable.currentRetry} retries.")  
          }
          stop()
     } 
     
     
     def stop() = {
          context.stop( context.self)
     }

}