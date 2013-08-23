package com.klout.satisfaction

import com.klout.scoozie._
import com.klout.scoozie.dsl._
import com.klout.scoozie.runner._
import com.klout.scoozie.jobs._

class ScoozieSatisfier(workflow: Workflow) extends Satisfier {

    val appPathParam: Variable[String] = new Variable[String]("scoozie.wf.application.path", classOf[String])
    val ScoozieUrlParam: Variable[String] = new Variable("scoozie.oozie.url", classOf[String])

    def satisfy(params: Substitution): Boolean = {
        try {
            val appPath = params.get(appPathParam).get
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