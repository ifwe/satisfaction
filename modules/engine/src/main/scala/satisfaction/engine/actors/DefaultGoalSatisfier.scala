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

class DefaultGoalSatisfier(
    val track : Track,
    val goal : Goal, 
    val evidence: Set[Evidence],
    val witness: Witness) extends Actor with ActorLogging {
  

    implicit val executionContext : ExecutionContext = ExecutionContext.global
    
    var remainingEvidence: Set[Evidence] = evidence
    var messageSender : ActorRef = null
    
    var execResult : ExecutionResult = null
    
    LogWrapper.modifyLogger( track)
    LogWrapper.modifyLogger( goal)
    val logger = new LogWrapper[ExecutionResult]( track, goal, witness)


    def receive = {
        case Satisfy =>
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
          logger.info(s"Still waiting on evidence: $remainingEvidence")
          println(s"Still waiting on evidence: $remainingEvidence")
          remainingEvidence foreach { evidenceToCheck =>
             if (evidenceToCheck.exists(witness)) {
                logger.info(s" Evidence is Now  there !!! ${evidenceToCheck} ")
                println(s" Evidence is Now  there !!! ${evidenceToCheck} ")
                 remainingEvidence = remainingEvidence - evidenceToCheck
              } else {
                logger.info(s" Evidence not there yet ${evidenceToCheck} ")
                Console.println(s" Evidence not there yet ${evidenceToCheck} ")
              }
          }
          remainingEvidence.isEmpty 
      }
    }

    def finish() {
        Console.println("No more evidence left.")
        logger.info("No more evidence left.")
        logger.close
        execResult.markSuccess
        if( messageSender != null) {
           messageSender !  new JobRunSuccess(execResult)
        }
    }

    def checkAgainLater() {
        context.system.scheduler.scheduleOnce(30 second, self, CheckEvidence)
    }

}