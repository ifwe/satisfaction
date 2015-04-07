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
 */
trait Retryable {
   
    val maxRetries : Int = 7;
    val waitPeriod : Duration = Duration.standardSeconds(30)
    val backOff    : Double = 1.5
    
    def retryNotifier : Option[Notifier] = None
    
    /**
     *  Override this method, if there is some logic
     *   so that only certain goals should be retried 
     */
    def shouldRetry( g : Goal ) : Boolean = true
}

