package com.klout.satisfaction;
package executor
package actors

import com.klout.satisfaction.Satisfier

/**
 *   Test class for Satisfier..
 *
 */
class MockSatisfier extends Satisfier with Evidence {
    var varMap = Set[String]()

    var retCode = true

    def satisfy(params: ParamMap): Boolean = {
        println(" Satisfy for params " + params.raw.mkString(","))
        varMap ++= params.raw.keySet
        println("  Returning code " + retCode)
        retCode
    }

    def exists(w: Witness): Boolean = {
        val xist = w.variables.keys.forall(v => varMap.contains(v))
        println(" Does the evidence exist for witness " + w.params.raw.mkString + " ???? " + xist)
        xist
    }

}