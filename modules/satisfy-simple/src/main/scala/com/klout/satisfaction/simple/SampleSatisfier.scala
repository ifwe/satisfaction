package com.klout
package satisfy
package track
package simple

import org.joda.time.DateTime

import com.klout.satisfaction._

/**
 *   Test class for Satisfier..
 *
 */
class SampleSatisfier(progressCount: Int, sleepTime: Long) extends Satisfier with Evidence {
    var varMap = Set[String]()

    var retCode = true
    var startTime : DateTime = null
    
    
    override def name = "SampleSatisfier"
    
    

    @Override
    override def satisfy(params: Witness): ExecutionResult = {
        startTime = new DateTime
        println(" Satisfy for params " + params.raw.mkString(","))
        varMap ++= params.raw.keySet

        for (i <- Range(0, progressCount)) {
            println("Doing the thing ;; Progress Count = " + i)
            Thread.sleep(sleepTime)
        }

        println("  Returning code " + retCode)
        val execResult = new ExecutionResult("Simple Satisfier", startTime)
        execResult.timeEnded = new DateTime
        execResult.isSuccess = retCode

        execResult
    }

    def exists(w: Witness): Boolean = {
        w.raw.keys.forall(v => varMap.contains(v))
    }


    @Override
    override def abort() : ExecutionResult = {
      val execResult = new ExecutionResult("Simple Satisfier", startTime )
      execResult.markFailure
      execResult.errorMessage = "Aborted by User"
      execResult
   }

}
