package com.klout.satisfaction
package executor
package actors

import org.joda.time.DateTime
import com.klout.satisfaction.MetricsCollection

/**
 * Information about the result of a single unit of work.
 * ie. a Single job or script, as compared to the full
 *  goal dependency tree
 * 
 */
case class ExecutionResult(
    val executionName : String,
    val timeStarted: DateTime,
    var timeFinished : DateTime ) {
  
   val metrics = new MetricsCollection( executionName)
   
   
   /**
    * 
    */
   var currentProgress : Double = 0.0
   var currentProgressRate : Double = 0.0
   
   /**
    *  Provide a path of where on HDFS the logs are stored 
    */
   var hdfsLogPath : String = null
  
}

