package com.klout.satisfaction.projects

import com.klout.satisfaction._
import com.klout.satisfaction.executor.actors._
import scalaxb._
import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import com.klout.klout_scoozie.maxwell.workflows.WaitForKsUidMapping
import scala.concurrent.Await
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import executor.Satisfaction

class TestSampleProjectSpec extends Specification {
    val NetworkAbbr = Variable[String]("network_abbr", classOf[String])
    val DoDistcpVar = Variable[Boolean]("doDistcp", classOf[Boolean])
    val runDateVar = Variable[String]("dt", classOf[String])

    /**
     * "TestSampleProjectSpec" should {
     * "run Maxwell" in {
     * val engine = new ProofEngine()
     *
     * val maxwell = MaxwellProject.Project
     *
     * val witness = Witness((MaxwellProject.dateParam -> "20130821"),
     * (MaxwellProject.networkParam -> "li"))
     *
     * val topLevelProject = MaxwellProject.calcScore
     *
     * val status = engine.satisfyGoal(topLevelProject, witness)
     *
     * status.state must_== GoalState.Success
     * }
     *
     */

    "WaitForKsUIDMapping" in {
        val engine = new ProofEngine()

        ///val waitForKSUID = MaxwellProject.WaitForKSUIDMappingGoal

        val waitForKSUID: Goal = ScoozieGoal(
            workflow = WaitForKsUidMapping.Flow,
            Set(HiveTable("bi_maxwell", "ksuid_mapping")))

        val witness = Witness((runDateVar -> "20130826"), MaxwellProject.serviceIdVar -> 1)

        Satisfaction.satisfyGoal(waitForKSUID, witness)

        true
    }

}