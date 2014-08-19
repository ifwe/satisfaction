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

object Notifier {
  
   /**
    *  For a Track, return the implied Notifier from Track properties 
    *
    */
    def apply( tr : Track ) : Notifier = {
       new EmailNotifier( host = tr.trackProperties.getProperty("satisfaction.smtp.host"),
            port = tr.trackProperties.getProperty("satisfaction.smtp.port").toInt,
            from = tr.trackProperties.getProperty("satisfaction.email.from", "satisfaction@tagged.com"),
            recipients = tr.trackProperties.getProperty("satisfaction.email.recipients", "jbanks@tagged.com,jerome_banks@yahoo.com").split(",").toSet
       )
    }
  
}