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
import satisfaction.fs._
import satisfaction.hadoop.hdfs.Hdfs
import satisfaction.engine.Satisfaction
import satisfaction.engine.actors.GoalState

/**
 *  Test that Hive Goals work with HiveGoals which have been loaded from
 *    TrackFactory 
 */


object MockTrackFactory( new LocalFileSystem( System.getProperty("user.dir") + "/src/test/resources")
    )  extends TrackFactory {
  
  
}

@RunWith(classOf[JUnitRunner])
class HiveTrackSpec extends Specification {
    val NetworkAbbr = new Variable[String]("network_abbr", classOf[String])
    val DoDistcp = new Variable[Boolean]("doDistcp", classOf[Boolean])
    val runDate = new Variable[String]("dt", classOf[String])
    
    implicit val ms : MetaStore = MetaStore( new java.net.URI("thrift://jobs-dev-sched2:9085") )
    implicit val hdfs : FileSystem = new Hdfs("hdfs://jobs-dev-hnn") 

    "HiveGoalSpec" should {
       "Run a Hive goal" in {
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val actorAction: Goal = HiveGoal(
                name = "Fact Content",
                queryResource ="fact_content.hql",
                table = HiveTable("bi_maxwell", "actor_action"),
                overrides = None, Set.empty);

            val witness = Witness((runDate -> "20140117"), (NetworkAbbr -> "li"))
            val goalResult = Satisfaction.satisfyGoal(actorAction, witness)

            goalResult.state == GoalState.Success
        }

    }
}