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

class WitnessGeneratorActor(
    witnessGenerator: WitnessGenerator) extends Actor with ActorLogging {

    var maybeLastWitness: Option[Witness] = None

    def receive = {
        case MaybeGenerateWitness =>
            val maybeNewWitness = witnessGenerator.generator apply maybeLastWitness
            maybeLastWitness = maybeNewWitness
            for (witness <- maybeNewWitness) {
                log.info(s"created new witness: $witness")
                context.parent ! NewWitnessGenerated(witness)
            }
    }

    override def preStart() {
        val cron = witnessGenerator.cronOverride getOrElse DefaultWitnessGeneratorCron
        import us.theatr.akka.quartz._
        val quartzActor = context.actorOf(Props[QuartzActor])
        quartzActor ! AddCronSchedule(self, cron, MaybeGenerateWitness)
    }
}
