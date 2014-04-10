package com.klout
package satisfaction
package engine
package actors


import com.klout.satisfaction.ExecutionResult
import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Future._
import scala.concurrent._
import scala.util.Try
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.File
import org.joda.time.DateTime
import scala.util.Success
import scala.util.Failure

class JobRunner(
    satisfier: Satisfier,
    track : Track,
    goal : Goal, 
    witness : Witness,
    params: Substitution ) extends Actor with ActorLogging {

  
    implicit val executionContext : ExecutionContext = ExecutionContext.global
    
    var satisfierFuture: Future[ExecutionResult] = null
    var messageSender: ActorRef = null
    var startTime : DateTime = null;
    val logger = new LogWrapper[ExecutionResult]( track, goal, witness)

    def receive = {
        case Satisfy =>
            log.info(s"Asked to satisfy for params: ${params}")
            startTime = DateTime.now

            if (satisfierFuture == null) {
                satisfierFuture = future {
                    logger.log( { () => satisfier.satisfy(params) } ) match {
                      case Success(execResult) =>
                        execResult.hdfsLogPath = logger.getHdfsLogPath 
                        execResult
                      case Failure(throwable) =>
                        //// Error occurred somehow because of logging,
                        ///   or from satisfier throwing unexpected exception
                        val execResult = new ExecutionResult(goal.name, startTime )
                        execResult.hdfsLogPath = logger.getHdfsLogPath 
                        execResult.markUnexpected( throwable)
                      
                    }
                }
                messageSender = sender
                satisfierFuture onComplete {
                    checkResults(_)
                }
            }
        case Abort =>
          log.warning(" Aborting Job !!!")
          try {
             val abortResult : ExecutionResult = satisfier.abort()
              checkResults( Success(abortResult))
          } catch {
            case unexpected : Throwable =>
              checkResults( Failure(unexpected))
          }
          
    }


    def checkResults(result: Try[ExecutionResult]) = {
        log.info("Sending GoalSatisfied to parent")
        log.info("Some result =  " + result)
        if (result.isSuccess) {
            val execResult = result.get
            execResult.hdfsLogPath = logger.getHdfsLogPath
            if (execResult.isSuccess ) {
                messageSender ! new JobRunSuccess(execResult)
            } else {
                log.info(" Bool is false " + execResult)
                messageSender ! new JobRunFailed(execResult)
            }
        } else {
             //// Error occurred with Akka Actors
            log.info(" result isFailure " + result.failed.get)
            val execResult = new ExecutionResult( "Failure executing Goal " + goal.name , new DateTime)
            execResult.hdfsLogPath = logger.getHdfsLogPath
            execResult.markUnexpected( result.failed.get)
            messageSender ! new JobRunFailed(execResult)
        }
    }

}
