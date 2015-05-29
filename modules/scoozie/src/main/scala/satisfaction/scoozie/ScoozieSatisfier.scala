package com.klout
package satisfaction
package hadoop
package scoozie

import com.klout.scoozie._
import com.klout.scoozie.dsl._
import com.klout.scoozie.runner._
import com.klout.scoozie.jobs._
import util.{ Left, Right }
import collection.JavaConversions._
import org.joda.time.DateTime
import org.apache.oozie.client.OozieClient

class ScoozieSatisfier(workflow: Workflow) (implicit track : Track) extends Satisfier {

    val appPathParam: Variable[String] = Variable("scoozie.wf.application.path")
    val ScoozieUrlParam: Variable[String] = Variable("scoozie.oozie.url")
    val appRootParam: Variable[String] = Variable("applicationRoot")
    var scoozieUrl : String = null

    @Override
    override def satisfy(params: Substitution): ExecutionResult = {
        val timeStarted  = new DateTime
        try {
            val allParams = params ++ track.getTrackProperties( params)

            if (!allParams.contains(appPathParam)) {
                throw new IllegalArgumentException("Must specify application path ")
            }
            val appPath = scoozieApplicationPath( allParams.get(appPathParam).get, params)
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
                    execResult.markFailure
                case Right(oozieSuccess) =>
                    println(s" Huzzah !!! Oozie job ${oozieSuccess.jobId} has completed with great success!!!")
                    val execResult = new ExecutionResult( workflow.name, timeStarted)
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
    
    override def abort() : ExecutionResult = {
      /// Can we abort the process ???
      /// Should be part of scoozie, but 
      ///   not currently accessible 
      if(scoozieUrl != null) {
      	val oozieClient = new OozieClient( scoozieUrl)
      	/// Somehow get the Job Id from the scoozie client ...
      	//// For now try killing jobs with that name 
      	//// Might not work with multiple oozies
      	oozieClient.getJobsInfo(s"NAME=${workflow.name}") foreach( wfj => {
           println(s" Killing Oozie WF ${wfj.getAppName} ${wfj.getId} ")
           oozieClient.kill( wfj.getId )
        })
      } 
      val execResult = new ExecutionResult( workflow.name, DateTime.now)
      execResult.markFailure
      
    }

    
    def scoozieApplicationPath( basePath : String, witness : Substitution ) : String = {
      val suffix = witness.pathString
      s"$basePath/scoozie_${workflow.name}_$suffix.xml"
    }

}