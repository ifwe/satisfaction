package com.klout.satisfaction
package executor
package actors

import org.joda.time.DateTime
import com.klout.satisfaction.MetricsCollection

/**
 *  Object which tells us about the execution run,
 *   ie. was it successful, what where any sub tasks,
 *   how long did it take, how much data was produced , 
 */
case class ExecutionResult(
    val executionName : String,
    val startTime: DateTime,
    val endTime : DateTime ) {
  
   val metrics = new MetricsCollection( executionName)
  
}

