package com.klout.satisfaction.projects

import com.klout.satisfaction._
import com.klout.satisfaction.executor.actors._
import scalaxb._
import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import com.klout.klout_scoozie.maxwell.workflows.WaitForKsUidMapping

class TestSampleProjectSpec extends Specification {
    object NetworkAbbr extends Param[String]("network_abbr")
    object DoDistcp extends Param[Boolean]("doDistcp")
    object runDate extends Param[String]("dt")

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

        val witness = Witness((MaxwellProject.dateParam -> "20130821"), MaxwellProject.serviceIDParam -> 1)

        val status = engine.satisfyGoal(waitForKSUID, witness)

        status.state must_== GoalState.Success
    }

}