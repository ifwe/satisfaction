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
    val hour = new Variable[String]("hour", classOf[String])
    val runDate = new Variable[String]("date", classOf[String])
    
    implicit val ms : MetaStore = MetaStore.default
    implicit val hdfs : FileSystem = Hdfs.default

    "HiveGoalSpec" should {
       "Run a Hive goal" in {
            implicit val track : Track = new Track(TrackDescriptor("HiveTrack"))
            val vars: List[Variable[_]] = List(hour, runDate)
            val actorAction: Goal = HiveGoal(
                name = "Dau By Platform",
                queryResource ="dau_by_platform.hql",
                table = HiveTable("sqoop_test", "dau_by_platform"),
                 Set.empty);

            val witness = Witness((runDate -> "20140420"), (hour -> "03"))
            val goalResult = Satisfaction.satisfyGoal(actorAction, witness)
            
            
            

            goalResult.state == GoalState.Success
        }

    }
}