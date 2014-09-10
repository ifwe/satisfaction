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
    var startTime : DateTime = null
    
    
    override def name = "MockSatisfier"

    @Override 
    def satisfy(params: Witness): ExecutionResult = {
        println(" Satisfy for params " + params.raw.mkString(","))
        startTime = DateTime.now
        varMap ++= params.raw.keySet
        println("  Returning code " + retCode)
        val execResult = new ExecutionResult("MockSatisfier", startTime )
        execResult.markSuccess
        
        
        execResult
    }
    
    @Override 
    override def abort() : ExecutionResult = {
      retCode = false 
      val abortResult = new ExecutionResult("MockSatisfier", startTime);
      abortResult.markSuccess
      abortResult
    }

    def exists(w: Witness): Boolean = {
        val xist = w.raw.keys.forall(v => varMap.contains(v))
        println(" Does the evidence exist for witness " + w.variables.mkString + " ???? " + xist)
        xist
    }

}
