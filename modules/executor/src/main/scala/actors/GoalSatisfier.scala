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

class GoalSatisfier(
    satisfier: Satisfier,
    params: ParamMap) extends Actor with ActorLogging {

    var satisfierFuture: Future[Boolean] = null
    var messageSender: ActorRef = null

    def receive = {
        case Satisfy =>
            log.info(s"Asked to satisfy for params: $params.mkString")

            // Actor receive blocks don't do well with long running processes.
            // Wrapping this in "blocking" may help deal with it's blocking nature.
            // We may also want to have this executed on a dedicated / shared thread pool
            // separate from this actor.
            // Potentially better would be to modify the Satisfer interface to
            // have a two step process: (1) start (2) check
            // Then we could implement it similarly to how we implement DefaultGoalSatisfier
            if (satisfierFuture == null) {
                satisfierFuture = future {
                    satisfier.satisfy(params)
                }
                messageSender = context.sender
                satisfierFuture onComplete {
                    checkResults(_)
                }
                ///messageSender = context.parent
            }

    }

    def checkResults(result: Try[Boolean]) = {
        log.info("Sending GoalSatisfied to parent")
        log.info("Some result =  " + result)
        if (result.isSuccess) {
            if (result.get) {
                messageSender ! GoalSatisfied
            } else {
                log.info(" Bool is false " + GoalFailed)
                messageSender ! GoalFailed
            }
        } else {
            log.info(" result isFailure " + result.failed.get)
            messageSender ! GoalFailed
        }
    }

}
