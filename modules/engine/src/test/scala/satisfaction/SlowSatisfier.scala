package satisfaction;
package engine
package actors

import satisfaction.Satisfier
import org.joda.time.DateTime

/**
 *   Test class for Satisfier..
 *
 */
class SlowSatisfier(progressCount: Int, sleepTime: Long) extends MockSatisfier with Evidence {
  
    override def name = "Slow Satisfier"
      var runningThread : Thread = null

    @Override
    override def satisfy(params: Witness) : ExecutionResult = {
        startTime = DateTime.now
        runningThread = Thread.currentThread()
        for (i <- Range(0, progressCount)) {
            println("Doing the thing ;; Progress Count = " + i)
            Thread.sleep(sleepTime)
            
        }
        super.satisfy(params)
    }
    
    
    @Override 
    override def abort() : ExecutionResult = {
      retCode = false
      runningThread.interrupt
      val abortResult = new ExecutionResult("MockSatisfier", startTime);
      abortResult.isSuccess = true 
      abortResult.timeEnded = DateTime.now
      abortResult
    }

}