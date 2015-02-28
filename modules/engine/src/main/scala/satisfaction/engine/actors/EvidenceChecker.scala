package satisfaction.engine.actors

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future
import scala.concurrent.Future
import akka.actor.Actor
import akka.actor.ActorLogging
import satisfaction.Evidence
import satisfaction.Track
import satisfaction.Witness
import satisfaction.ExecutionResult
import org.joda.time.DateTime
import akka.actor.ActorRef

/**
 *  EvidenceChecker checks evidence
 *     for a Goal, to see if it has already been satisfied.
 *     
 *  This is implemented as a separate Actor, 
 *    in case the check takes a while
 *   ( because it is checking some external 
 *      file sytem or database )
 *  So that it doesn't block the checking of status 
 *   or any other actor activity    
 */
class EvidenceChecker(val e : Evidence) extends Actor  with satisfaction.Logging {

  private var  _sender : ActorRef = null
  def receive = {
    case CheckEvidence(evidenceId,witness) =>
       info(s" CHECKING EVIDENCE $e for %witness with id $evidenceId ")
       val evidenceFuture: Future[Boolean] = future {
          JobRunner.threadPreserve(e) {  e.exists( witness) }
       }
       _sender = context.sender
       evidenceFuture.onSuccess({
         case exists : Boolean => {
            info(s" Sending EvidenceCheckResult $evidenceId $witness EXISTS = $exists")
            _sender !  EvidenceCheckResult( evidenceId, witness, exists)  
       }})
       evidenceFuture.onFailure( { 
         case unexpected : Throwable =>  {
           /////Publish JobRunFailed, if we are unable to check the evidence
           ////  Even if
           log.error(s" Unexpected error while checking evidence for $e for witness $witness ; ${unexpected.getMessage()}", unexpected )
           error(s" Unexpected error while checking evidence for $e for witness $witness ; ${unexpected.getMessage()}", unexpected )
           val er = new ExecutionResult(s"Evidence Checking $e $witness", DateTime.now)
           er.markUnexpected(unexpected)
           val failed = new JobRunFailed( er)
           _sender ! failed
           
         }
       })
  } 

   def stop() = {
     context.stop( context.self)
   }

}