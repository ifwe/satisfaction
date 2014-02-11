package com.klout
package satisfaction
package engine
package actors


import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import org.joda.time.DateTime


/**
 * When there is no Satisfier configured for a Goal, we use this one.
 *
 * It simply waits for all of the evidence to be completed.
 *
 * Implementation Note: It is MUCH better to check and schedule future checks
 * rather than to call Thread.sleep(), hogging the thread.
 */

/**
case class WaitingSatisfier(val evidence : Set[Evidence]) extends Satisfier {
  
    var remainingEvidence : Set[Evidence]  = evidence
    var execResult= new ExecutionResult("Waiting ", DateTime.now)
    def satisfy(subst: Substitution): ExecutionResult = {
     
      while( { ! remainingEvidence.isEmpty } ) {
                remainingEvidence foreach { evidenceToCheck =>
                    if (evidenceToCheck.exists(new Witness(subst))) {
                        remainingEvidence - evidenceToCheck
                    }
                }
      
                Thread.sleep(10000)
      }
      execResult.markSuccess
    }
    

  
}
* **
*/
class DefaultGoalSatisfier(
     evidence: Set[Evidence],
    witness: Witness) extends Actor with ActorLogging {
  
   /// XXX Use LogWrapper to preserve time logs ???

    implicit val executionContext : ExecutionContext = ExecutionContext.global
    
    var remainingEvidence: Set[Evidence] = evidence
    var messageSender : ActorRef = null
    
    var execResult : ExecutionResult = null

    def receive = {
        case Satisfy =>
            ///log.info(s"Asked to satisfy for params: $params")
          messageSender = sender
          execResult = new ExecutionResult( evidence.mkString("_"), DateTime.now)
          if( checkEvidence() ) {
             finish()            
          } else {
            checkAgainLater()
          }

        case CheckEvidence =>
          if( checkEvidence() ) {
             finish()            
          } else {
            checkAgainLater()
          }
    }
    
    
    def checkEvidence() : Boolean = {
      if( remainingEvidence.isEmpty) {
        true
      } else {
          log.info(s"Still waiting on evidence: $remainingEvidence")
          remainingEvidence foreach { evidenceToCheck =>
             if (evidenceToCheck.exists(witness)) {
                log.info(s" Evidence is Now  there !!! ${evidenceToCheck} ")
                 remainingEvidence = remainingEvidence - evidenceToCheck
              } else {
                log.info(s" Evidence not there yet ${evidenceToCheck} ")
              }
          }
          remainingEvidence.isEmpty 
      }
    }

    def finish() {
        log.info("No more evidence left.")
        execResult.markSuccess
        if( messageSender != null) {
           messageSender !  new JobRunSuccess(execResult)
        }
    }

    def checkAgainLater() {
        ///context.system.scheduler.scheduleOnce(1.minute, self, CheckEvidence)
        log.info(" Check again LATER !!!")
        context.system.scheduler.scheduleOnce(30 second, self, CheckEvidence)
      
    }

}