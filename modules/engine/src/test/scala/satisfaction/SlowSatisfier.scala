package com.klout
package satisfaction;
package engine
package actors

import com.klout.satisfaction.Satisfier

/**
 *   Test class for Satisfier..
 *
 */
class SlowSatisfier(progressCount: Int, sleepTime: Long) extends MockSatisfier with Evidence {

    @Override
    override def satisfy(params: Substitution) : ExecutionResult = {
        for (i <- Range(0, progressCount)) {
            println("Doing the thing ;; Progress Count = " + i)
            Thread.sleep(sleepTime)
        }
        super.satisfy(params)
    }

}