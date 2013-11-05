package com.klout.satisfaction
package executor
package actors

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

class JobRunner(
    satisfier: Satisfier,
    track : Track,
    goal : Goal, 
    witness : Witness,
    params: Substitution ) extends Actor with ActorLogging {

    var satisfierFuture: Future[Boolean] = null
    var messageSender: ActorRef = null
    val logger = new LogWrapper[Boolean]( track, goal, witness)
    var startTime : DateTime = null
    var endTime : DateTime = null

    def receive = {
        case Satisfy =>
            log.info(s"Asked to satisfy for params: ${params}")

            if (satisfierFuture == null) {
                startTime = DateTime.now
                satisfierFuture = future {
                    logger.log( { () => satisfier.satisfy(params) } ) match {
                      case Some(result) => result
                      case None => false
                    }
                }
                messageSender = sender
                satisfierFuture onComplete {
                    endTime = DateTime.now
                    checkResults(_)
                }
            }
          
    }


    def checkResults(result: Try[Boolean]) = {
        log.info("Sending GoalSatisfied to parent")
        log.info("Some result =  " + result)
        val execResult = new ExecutionResult( goal.name, startTime, endTime)
        if (satisfier.isInstanceOf[MetricsProducing]) {
        	val metricsSatisfier = satisfier.asInstanceOf[MetricsProducing]
        	execResult.metrics.mergeMetrics( metricsSatisfier.jobMetrics )
        }
        execResult.hdfsLogPath = logger.getHdfsLogPath
        if (result.isSuccess) {
            if (result.get) {
                messageSender ! new JobRunSuccess(execResult)
            } else {
                log.info(" Bool is false " + execResult)
                messageSender ! new JobRunFailed(execResult)
            }
        } else {
            log.info(" result isFailure " + result.failed.get)
            messageSender ! new JobRunFailed(execResult)
        }
    }

}
