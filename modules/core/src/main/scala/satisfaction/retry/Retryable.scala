package satisfaction
package retry

import org.joda.time.Duration
import satisfaction.notifier.Notifier
import satisfaction.Goal

/**
 *  Retryable on a Goal or Track 
 *   implies that the job should 
 *    be attempted 
 *  
 *  XXX TODO 
 *  XXX Allow logic for exponential backoff
 *  XXX  and for scanning output A
 *  XXX  to decide if retry is desired ...
 */
trait Retryable {
   
    val maxRetries : Int = 3;
    val waitPeriod : Duration = Duration.standardSeconds(30)
    
    var currentRetry = 0
    
    def notifier : Option[Notifier] = None
}

/**
 *  Some Scala magic to add the trait 
 *    to existing Goals ...
 */
object Retryable {
  
  class RetryableGoal( g : Goal )
           (implicit track : Track ) extends Goal( g.name, g.satisfier,
    		   	g.variables,
    		   	g.dependencies,
    		   	g.evidence ) with Retryable {
     
        override val maxRetries = track.trackProperties.getProperty( "satisfaction.retry.maxRetries", "3").toInt
        override def notifier =  {
             Some(Notifier(track))
        }
    }
     
  
   def ::(g : satisfaction.Goal )(implicit track : Track) : RetryableGoal = { new RetryableGoal( g )(track) }
  
}