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
    val timeStarted: DateTime = DateTime.now) {
  
   private var _timeEnded : DateTime = null 
   def timeEnded : DateTime = _timeEnded

   var _isSuccess : Boolean = false /// XXX make stateless .. Add is running ???
   def isSuccess = _isSuccess
   
   
   val metrics = new MetricsCollection( executionName)
   
   
   @Override
   override def toString() = {
     s"ExecResult( $executionName, SUCCESS=$isSuccess, START=$timeStarted, END =$timeEnded"
     ///XXX add stack trace or metrics
   }
   
   def markSuccess() :ExecutionResult = {
      _isSuccess = true
      _timeEnded = DateTime.now 
      this
   }
  
   def markFailure( errMess : String = null) : ExecutionResult = {
      _isSuccess = false 
      _timeEnded = DateTime.now
      _errorMessage = errMess
      this
   }
   
   def markUnexpected( exc : Throwable ) : ExecutionResult = {
     _isSuccess = false
     _timeEnded = DateTime.now
     _errorMessage = exc.getMessage
     _stackTrace = exc.getStackTrace 
     this
   }
   
   
   /**
    *  Provide a path of where on HDFS the logs are stored 
    *  XXX Change to URI
    *  XXXX decouple from ExecResult ...
    */
   var hdfsLogPath : String = null
   
   /**
    * An error message if the result was successful
    */
   private var _errorMessage : String  = null
   def errorMessage = _errorMessage
   
   
   /**
    *  Possible stack trace for something which went wrong 
    */
   private var _stackTrace : Array[StackTraceElement] = null
   def stackTrace = _stackTrace
  
}

