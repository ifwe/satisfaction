package com.klout
package satisfaction

import org.joda.time.DateTime

/**
 * Information about the result of a single unit of work.
 * ie. a Single job or script, as compared to the full
 *  goal dependency tree
 * 
 */
case class ExecutionResult(
    val executionName : String,
    val timeStarted: DateTime) {
  
   var timeEnded : DateTime = null
   var isSuccess : Boolean = false
   
   val metrics = new MetricsCollection( executionName)
   
   
   
   def markSuccess() :ExecutionResult = {
      isSuccess = true
      timeEnded = DateTime.now 
      this
   }
  
   def markFailure() : ExecutionResult = {
      isSuccess = false 
      timeEnded = DateTime.now
      this
   }
   
   def markUnexpected( exc : Throwable ) : ExecutionResult = {
     isSuccess = false
     timeEnded = DateTime.now
     errorMessage = exc.getMessage
     stackTrace = exc.getStackTrace 
     this
   }
   
   
   /**
    *  Provide a path of where on HDFS the logs are stored 
    *  XXX Change to URI
    */
   var hdfsLogPath : String = null
   
   /**
    * An error message if the result was successful
    */
   var errorMessage : String  = null
   
   
   /**
    *  Possible stack trace for something which went wrong 
    */
   var stackTrace : Array[StackTraceElement] = null
  
}

