package com.klout
package satisfaction;
package engine
package actors

import com.klout.satisfaction.Satisfier
import org.joda.time.DateTime

/**
 *   Test class for Satisfier..
 *
 */
class SlowSatisfier(progressCount: Int, sleepTime: Long) extends MockSatisfier with Evidence {
  
    override def name = "Slow Satisfier"

    @Override
    override def satisfy(params: Substitution) : ExecutionResult = {
        startTime = DateTime.now
        for (i <- Range(0, progressCount)) {
            println("Doing the thing ;; Progress Count = " + i)
            Thread.sleep(sleepTime)
        }
        super.satisfy(params)
    }
    
    
      @Override 
    override def abort() : ExecutionResult = {
      val abortResult = new ExecutionResult("MockSatisfier", startTime);
      abortResult.isSuccess = false;
      abortResult.timeEnded = DateTime.now
      abortResult
    }

}