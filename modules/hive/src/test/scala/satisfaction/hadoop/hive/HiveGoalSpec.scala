package com.klout
package satisfaction
package hadoop
package hive

import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level
import hadoop.hive.ms._
import satisfaction.fs.FileSystem
import satisfaction.hadoop.hdfs.Hdfs
import satisfaction.engine.Satisfaction
import satisfaction.engine.actors.GoalState


@RunWith(classOf[JUnitRunner])
class HiveGoalSpec extends Specification {
    val NetworkAbbr = new Variable[String]("network_abbr", classOf[String])
    val DoDistcp = new Variable[Boolean]("doDistcp", classOf[Boolean])
    val runDate = new Variable[String]("dt", classOf[String])
    
    implicit val ms : MetaStore = MetaStore( new java.net.URI("thrift://jobs-dev-sched2:9085") )
    implicit val hdfs : FileSystem = new Hdfs("hdfs://jobs-dev-hnn") 

    "HiveGoalSpec" should {
       "Run a Hive goal" in {
            implicit val track : Track = new Track(TrackDescriptor("HiveTrack"))
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val actorAction: Goal = HiveGoal(
                name = "Fact Content",
                queryResource ="fact_content.hql",
                table = HiveTable("bi_maxwell", "actor_action"),
                 Set.empty);

            val witness = Witness((runDate -> "20140117"), (NetworkAbbr -> "li"))
            val goalResult = Satisfaction.satisfyGoal(actorAction, witness)

            goalResult.state == GoalState.Success
        }

    }
}