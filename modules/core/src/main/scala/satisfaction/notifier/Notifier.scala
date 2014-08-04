package satisfaction.notifier

import satisfaction.ExecutionResult
import satisfaction.Witness
import satisfaction.Track

/**
 *  Notifiers notify external resources 
 *   about a Goal satisfaction attempt.
 */

trait Notifier {
  
     def notify( witness  : Witness , result : ExecutionResult )(implicit track : Track)

}