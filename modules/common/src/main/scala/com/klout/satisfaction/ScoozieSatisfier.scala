package com.klout.satisfaction

import com.klout.scoozie._
import com.klout.scoozie.dsl._
import com.klout.scoozie.runner._
import com.klout.scoozie.jobs._

class ScoozieSatisfier(workflow: Workflow) extends Satisfier {

    object AppPathParam extends Param[String]("scoozie.wf.application.path")
    object ScoozieUrlParam extends Param[String]("scoozie.oozie.url")

    def satisfy(params: ParamMap): Boolean = {
        try {
            val appPath = params.get(AppPathParam).get
            val scoozieUrl = params.get(ScoozieUrlParam).get

            val oozieConfig = OozieConfig(scoozieUrl, params.raw)
            RunWorkflow(workflow, appPath, oozieConfig, None)
            true
        } catch {
            case unexpected: Throwable =>
                println(" Unexpected exception " + unexpected)
                false
        }
    }

}