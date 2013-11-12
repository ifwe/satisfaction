package com.klout.satisfaction

import com.klout.scoozie._
import com.klout.scoozie.dsl._
import com.klout.scoozie.runner._
import com.klout.scoozie.jobs._
import util.{ Left, Right }
import org.joda.time.DateTime

class ScoozieSatisfier(workflow: Workflow) extends Satisfier with TrackOriented {

    val appPathParam: Variable[String] = Variable("scoozie.wf.application.path")
    val ScoozieUrlParam: Variable[String] = Variable("scoozie.oozie.url")
    val appRootParam: Variable[String] = Variable("applicationRoot")

    @Override
    override def satisfy(params: Substitution): ExecutionResult = {
        val timeStarted  = new DateTime
        try {
            val allParams = massageProperties(params ++ getProjectProperties)

            if (!allParams.contains(appPathParam)) {
                throw new IllegalArgumentException("Must specify application path ")
            }
            val appPath = allParams.get(appPathParam).get
            println(" Application path = " + appPath)
            if (!allParams.contains(ScoozieUrlParam)) {
                throw new IllegalArgumentException("Must specify Oozie URL ")
            }
            val scoozieUrl = allParams.get(ScoozieUrlParam).get
            println("Oozie URL = " + scoozieUrl)

            if (!allParams.contains(ScoozieUrlParam)) {
                throw new IllegalArgumentException("Must specify Oozie URL ")
            }
            val oozieConfig = OozieConfig(scoozieUrl, allParams.raw)
            RunWorkflow(workflow, appPath, oozieConfig, None) match {
                case Left(oozieFail) =>
                    //// XXX TODO ... more diagnostic option
                    println(s" Oozie job ${oozieFail.jobId} failed miserably  !!!")
                    println(s" Console available at ${oozieFail.consoleUrl})")
                    val execResult = new ExecutionResult(oozieFail.jobId , timeStarted)
                    
                    execResult.errorMessage = oozieFail.jobLog
                      execResult
                case Right(oozieSuccess) =>
                    println(s" Huzzah !!! Oozie job ${oozieSuccess.jobId} has completed with great success!!!")
                    val execResult = new ExecutionResult( oozieSuccess.jobId, timeStarted)
                    execResult.markSuccess
            }
        } catch {
            case unexpected: Throwable =>
                println(" Unexpected exception " + unexpected)
                unexpected.printStackTrace()
                val execResult = new ExecutionResult( "Job Failed " + workflow.name, timeStarted )
                execResult.markUnexpected( unexpected)
        }
    }

    /**
     *  Translate "dt" to "dateString"
     *  Not sure if this should be "Klout-Specific"
     *   or there is "oozie-specific" logic we need to set
     *   for now , just to make sure that
     *     oozie gets date string correctly
     *   also add yesterdayString for oozie specific logic
     *
     *   XXX
     *   FIXME
     */
    //// XXX
    def massageProperties(params: Substitution): Substitution = {
        params.update(VariableAssignment("dateString", params.get(Variable("dt")).get)).
            update(VariableAssignment("networkAbbr", params.get(Variable("network_abbr"))))
    }

    def getProjectProperties: Substitution = {
        Substitution(Helpers.readProperties("modules/samples/src/test/resources/maxwell.properties"))
    }

}