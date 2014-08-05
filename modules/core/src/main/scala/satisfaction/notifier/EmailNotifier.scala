package satisfaction.notifier

import satisfaction.ExecutionResult
import satisfaction.Witness
import org.apache.commons.mail._
import satisfaction.Track
import collection.JavaConversions._
import java.net.InetAddress


trait EmailNotified  extends Notified {
    implicit val track : Track
  
    override def notifier = new EmailNotifier(hostname, smtpPort ,from, recipients)
  
    /**
     *  The hostname of the SMTP server 
     */ 
    def hostname : String = track.trackProperties.getProperty("satisfaction.smtp.host")
    def smtpPort : Int =  track.trackProperties.getProperty("satisfaction.smtp.port", "567").toInt
        
    def from : String = track.trackProperties.getProperty("satisfaction.email.from", "satisfaction@tagged.com")
    
    /**
     *   The recipients  to send email
     *    on the jobs success
     */
    def recipients : Set[String]  = Set.empty
    
    
}


/**
 *   XXX Simple email notifier ...
 *     Extend later with fancier formatting
 *     Bring GoalStatus to satisfaction-Core ...
 */
class EmailNotifier( val host: String, val port : Int, from : String, recipients :Set[String]) extends Notifier {
  
  @Override
  override def notify( witness : Witness , result : ExecutionResult )(implicit track : Track) = {
    val email = new SimpleEmail();
    email.setHostName(host)
    email.setSmtpPort( port)
    
    val failSucceed = if(result.isSuccess ) { "succeeded" } else { "failed" }
        
    val headLine = s"${result.executionName} ${failSucceed} ${witness.toString} "
    
    email.setSubject( headLine)
    email.setFrom( from)
    recipients.foreach( sendTo => { email.addTo(sendTo) } )
    
    val msg = new StringBuilder()
    msg.append(s" StartTime ${result.timeStarted} :: EndTime ${result.timeEnded} \n")
    msg.append(s" Witness  ${witness.toString} ")
    msg.append(s"  ${result.errorMessage} ")
    
    email.setMsg( msg.toString)
    
    
    email.send
  }

}