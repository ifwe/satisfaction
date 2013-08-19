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

class GoalSatisfier(
    satisfier: Satisfier,
    params: ParamMap) extends Actor with ActorLogging {

    def receive = {
        case SatisfyGoal =>
            log.info(s"Asked to satisfy for params: $params")

            // Actor receive blocks don't do well with long running processes.
            // Wrapping this in "blocking" may help deal with it's blocking nature.
            // We may also want to have this executed on a dedicated / shared thread pool
            // separate from this actor.
            // Potentially better would be to modify the Satisfer interface to
            // have a two step process: (1) start (2) check
            // Then we could implement it similarly to how we implement DefaultGoalSatisfier
            scala.concurrent.blocking {
                satisfier.satisfy(params)
            }

            context.parent ! GoalSatisfied
    }

}
