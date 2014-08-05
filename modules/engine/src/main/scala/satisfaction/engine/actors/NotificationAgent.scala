package satisfaction.engine.actors

import satisfaction.notifier.Notifier
import akka.actor.Actor
import akka.actor.ActorLogging
import satisfaction.Track
import satisfaction.notifier.Notified

/**
 *   First pass at notification 
 */
class NotificationAgent( notifier : Notifier )(implicit val track : Track ) extends Actor with ActorLogging {
  
     def notified : Notified =  {
        track match {
          case notified : Notified => {
              notified 
          }  
          case _ => {
            /// Shouldn't happen
            null
          }
        } 
     }
      
     def receive = {
        case GoalFailure(goalStatus) =>
          if( notified.notifyOnFailure)
            notify( goalStatus)
        case GoalSuccess(goalStatus) =>
          if( notified.notifyOnSuccess)
            notify( goalStatus)
     } 
     
     def notify( gs : GoalStatus ) {
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
       //// Set up for just one notification 
        context.stop( context.self)
     }

}