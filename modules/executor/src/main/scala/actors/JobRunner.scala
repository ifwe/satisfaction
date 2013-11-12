package com.klout.satisfaction
package executor
package actors


import com.klout.satisfaction.ExecutionResult
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits._
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

    var satisfierFuture: Future[ExecutionResult] = null
    var messageSender: ActorRef = null
    val logger = new LogWrapper[ExecutionResult]( track, goal, witness)

    def receive = {
        case Satisfy =>
            log.info(s"Asked to satisfy for params: ${params}")

            if (satisfierFuture == null) {
                if(satisfier.isInstanceOf[TrackOriented]) {
                  val trackCast : TrackOriented = satisfier.asInstanceOf[TrackOriented]
                  trackCast.setTrack(track)
                }
                satisfierFuture = future {
                    logger.log( { () => satisfier.satisfy(params) } ) match {
                      case Success(execResult) =>
                        execResult.hdfsLogPath = logger.getHdfsLogPath 
                        execResult
                      case Failure(throwable) =>
                        //// Error occurred somehow because of logging,
                        ///   or from satisfier
                        val execResult = new ExecutionResult(goal.name, new DateTime )
                        execResult.hdfsLogPath = logger.getHdfsLogPath 
                        execResult.markUnexpected( throwable)
                      
                    }
                }
                messageSender = sender
                satisfierFuture onComplete {
                    checkResults(_)
                }
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
