package satisfaction
package retry

import org.joda.time.Duration
import scala.annotation._
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
    
    def retryNotifier : Option[Notifier] = None
    
    /**
     *  Override this method, if there is some logic
     *   so that only certain goals should be retried 
     */
    def shouldRetry( g : Goal ) : Boolean = true
}

