package com.klout
package satisfaction
package engine
package actors

import org.joda.time.DateTime

///import com.klout.satisfaction.Satisfier

/**
 *   Test class for Satisfier..
 *
 */
class MockSatisfier extends Satisfier with Evidence {
    var varMap = Set[String]()

    var retCode = true

    @Override 
    def satisfy(params: Substitution): ExecutionResult = {
        println(" Satisfy for params " + params.raw.mkString(","))
        varMap ++= params.raw.keySet
        println("  Returning code " + retCode)
        val execResult = new ExecutionResult("MockSatisfier", new DateTime )
        execResult.isSuccess = retCode
        execResult.timeEnded  = new DateTime
        
        
        execResult
    }

    def exists(w: Witness): Boolean = {
        val xist = w.substitution.raw.keys.forall(v => varMap.contains(v))
        println(" Does the evidence exist for witness " + w.variables.mkString + " ???? " + xist)
        xist
    }

}