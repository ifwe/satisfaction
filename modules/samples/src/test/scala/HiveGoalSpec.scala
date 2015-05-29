package com.klout.satisfaction
package executor
package actors

import scalaxb._
import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import com.klout.klout_scoozie.common.Network
import com.klout.klout_scoozie.common.Networks
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level

@RunWith(classOf[JUnitRunner])
class HiveGoalSpec extends Specification {
    val NetworkAbbr = new Variable[String]("network_abbr", classOf[String])
    val DoDistcp = new Variable[Boolean]("doDistcp", classOf[Boolean])
    val runDate = new Variable[String]("dt", classOf[String])

    "HiveGoalSpec" should {
        "Run a Hive goal" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val networkName = Networks.LinkedIn
            val actorAction: Goal = HiveGoal(
                name = "Fact Content",
                query = HiveGoal.readResource("fact_content.hql"),
                table = HiveTable("bi_maxwell", "actor_action"),
                overrides = None, Set.empty);

            val witness = Witness((runDate -> "20130910"), (NetworkAbbr -> "li"))
            ///val result = engine.satisfyProject(project, witness)
            Satisfaction.satisfyGoal(actorAction, witness)

        }

    }
}