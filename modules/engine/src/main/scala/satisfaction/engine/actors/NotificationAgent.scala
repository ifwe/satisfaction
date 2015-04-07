package satisfaction
package engine
package actors

import satisfaction.notifier.Notifier
import akka.actor.Actor
import akka.actor.ActorLogging
import satisfaction.Track
import satisfaction.notifier.Notified
import GoalStatus._


/**
 *   First pass at notification 
 */
class NotificationAgent( val notified : Notified ) extends Actor with ActorLogging {
  
     val notifier = notified.notifier
     
     /**
      *   Notified has to be a track
      */ 
     implicit val track : Track = notified.asInstanceOf[Track]
      
     def receive = {
        case GoalFailure(goalStatus) =>
          if( notified.notifyOnFailure) {
            if( goalStatus.state == GoalState.Failed
                || goalStatus.state == GoalState.Aborted) {
              notify( goalStatus)
            }
          }
         /// Continue to listen on failure, 
         /// in case there are retries ...
        case GoalSuccess(goalStatus) => {
            if( notified.notifyOnSuccess) {
               if( goalStatus.state == GoalState.Success) {
                  notify( goalStatus)
               }
             }
            stop()
        }
     } 

     
     def notify( gs : GoalStatus ) = {
       log.info(s" Notifying result of ${gs.goalName} is ${gs.state} ")
       //// XXXX Do retry logic for notification errors ...
       //// XXX If email is temporarily down because of network issues,
       ////   we want to retry !!!
       try { 
          notifier.notify( gs.witness, gs.execResult )
       } catch {
         case unexpected : Throwable => {
           log.error("Unexpected error while notifying job status ", unexpected)
         } 
       }
     }
     
     def stop() = {
        context.stop( context.self)
     }

}