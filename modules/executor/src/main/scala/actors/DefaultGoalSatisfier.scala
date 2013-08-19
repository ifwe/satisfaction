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

/**
 * When there is no Satisfier configured for a Goal, we use this one.
 * It simply waits for all of the evidence to be completed.
 *
 * Implementation Note: It is MUCH better to check and schedule future checks
 * rather than to call Thread.sleep(), hogging the thread.
 */
class DefaultGoalSatisfier(
    evidence: Set[Evidence],
    params: ParamMap) extends Actor with ActorLogging {

    var remainingEvidence: Set[Evidence] = evidence

    def receive = {
        case SatisfyGoal =>
            log.info(s"Asked to satisfy for params: $params")
            checkAgainLater()

        case CheckEvidence =>
            if (remainingEvidence.isEmpty) {
                finished()
            } else {
                log.info(s"Still waiting on evidence: $evidence")
                remainingEvidence foreach { evidenceToCheck =>
                    if (evidenceToCheck.exists(params)) {
                        remainingEvidence - evidenceToCheck
                    }
                }
                if (evidence.isEmpty) {
                    finished()
                } else {
                    checkAgainLater()
                }
            }
    }

    def finished() {
        log.info("No more evidence left.")
        context.parent ! GoalSatisfied
    }

    def checkAgainLater() {
        context.system.scheduler.scheduleOnce(1.minute, self, CheckEvidence)
    }

}