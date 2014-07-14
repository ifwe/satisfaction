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

/**
 *  JobRunner actually satisfies a goal,
 *   most likely by running some sort of job.
 */
class JobRunner(
    val satisfier: Satisfier,
    val track : Track,
    val goal : Goal, 
    val witness : Witness,
    params: Witness ) extends Actor with ActorLogging {

  
    implicit val executionContext : ExecutionContext = ExecutionContext.global
    
    var messageSender: ActorRef = null
    var startTime : DateTime = null;
    LogWrapper.modifyLogger( track)
    LogWrapper.modifyLogger( goal)
    LogWrapper.modifyLogger( satisfier)
    val logger = new LogWrapper[ExecutionResult]( track, goal, witness)

    def receive = {
        case Satisfy =>
            log.info(s"Asked to satisfy ${track.descriptor.trackName} :: ${goal.name} for params: ${params}")
            startTime = DateTime.now

                val satisfierFuture = future {
                    logger.log { () => satisfier.satisfy(params) } match {
                      case Success(execResult) =>
                        execResult.hdfsLogPath = logger.hdfsLogPath.toString
                        execResult
                      case Failure(throwable) =>
                        //// Error occurred somehow because of logging,
                        ///   or from satisfier throwing unexpected exception
                        val execResult = new ExecutionResult(goal.name, startTime )
                        execResult.hdfsLogPath = logger.hdfsLogPath.toString
                        execResult.markUnexpected( throwable)
                    }
                }
                messageSender = sender
                satisfierFuture onComplete {
                    checkResults(_)
                }
        case Abort =>
          log.warning(s" Aborting Job ${goal.name} !!!")
          Console.println(s" Aborting Job ${goal.name} !!!")
          /// For now Aborts are fire and forget
          ///  Assume Abort has completed.
          //// TODO Wait for abort result, and send to parents

          val abortResult : ExecutionResult = satisfier.abort()
          log.info( "Result of Abort Attempt is " + abortResult)
          Console.println( "Result of Abort Attempt is " + abortResult)
          /**
          try {
             val abortResult : ExecutionResult = satisfier.abort()
              checkResults( Success(abortResult))
          } catch {
            case unexpected : Throwable =>
              checkResults( Failure(unexpected))
          }
          * 
          */
          
    }


    def checkResults(result: Try[ExecutionResult]) = {
        if (result.isSuccess) {
            val execResult = result.get
            execResult.hdfsLogPath = logger.hdfsLogPath.toString
            if (execResult.isSuccess ) {
                messageSender ! new JobRunSuccess(execResult)
            } else {
                messageSender ! new JobRunFailed(execResult)
            }
        } else {
             //// Error occurred with Akka Actors
            log.info(" result isFailure " + result.failed.get)
            val execResult = new ExecutionResult( "Failure executing Goal " + goal.name , new DateTime)
            execResult.hdfsLogPath = logger.hdfsLogPath.toString
            execResult.markUnexpected( result.failed.get)
            messageSender ! new JobRunFailed(execResult)
        }
    }

}
