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
    
    

    @Override
    override def satisfy(params: Substitution): ExecutionResult = {
        val startTime = new DateTime
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
        val xist = w.substitution.raw.keys.forall(v => varMap.contains(v))
        println(" Does the evidence exist for witness " + w.variables.mkString + " ???? " + xist)
        xist
    }

}
