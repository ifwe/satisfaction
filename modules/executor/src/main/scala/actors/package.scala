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

object `package` {

    implicit val timeout = Timeout(5.seconds)

    val system = ActorSystem("projects")

    lazy val fs = {
        val conf = new Configuration()
        ///conf.set("fs.default.name", "jobs-aa-hnn:8020")
        conf.set("fs.default.name", "jobs-dev-hnn:8020")
        val fs = FileSystem.get(conf)
        fs
    }

    val DefaultWitnessGeneratorCron = "0 0 * * * *"
}