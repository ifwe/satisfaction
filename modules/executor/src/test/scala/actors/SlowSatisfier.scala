package com.klout.satisfaction;
package executor
package actors

import com.klout.satisfaction.Satisfier

/**
 *   Test class for Satisfier..
 *
 */
class SlowSatisfier(progressCount: Int, sleepTime: Long) extends MockSatisfier with Evidence {

    override def satisfy(params: ParamMap) = {
        for (i <- Range(0, progressCount)) {
            println("Doing the thing ;; Progress Count = " + i)
            Thread.sleep(sleepTime)
        }
        super.satisfy(params)
    }

}