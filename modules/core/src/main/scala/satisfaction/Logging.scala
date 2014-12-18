package satisfaction

import org.slf4j.{Logger, LoggerFactory}
 
trait Logging {
  lazy val log = LoggerFactory.getLogger(getClass)
 
  //// AY YAH ... yields AbstractMethodError !!!!
  def wrapExc( f :  => Unit) : Unit = {
    try {
       return f
    } catch {
      case illin : IllegalStateException => {
         println(" We Be illin ", illin.getMessage() )
         illin.printStackTrace(System.out) 
      }
    }
  }
 
 def trace(message:String, values:Any*) =   {
     log.trace(message, values.map(_.asInstanceOf[Object]).toArray)
 } 
 def trace(message:String, error:Throwable) =  {
   log.trace(message, error)
 } 
 
 def debug(message:String, values:Any*) =  {
     log.debug(message, values.map(_.asInstanceOf[Object]).toArray)
 } 
 def debug(message:String, error:Throwable) =  {
   log.debug(message, error)
 } 
 
 def info(message:String, values:Any*) =   {
   try {
     log.info(message, values.map(_.asInstanceOf[Object]).toArray)
     
     Console.println( s"INFO - SATISFACTION $message")
   } catch {
     case illin : IllegalStateException => {
       System.out.println(" Illegal State Exception while attempting to log !!!! ", illin) 
       illin.printStackTrace(System.out)
       System.out.println(" Original Message was " + message)
     }
     
   }
  } 
 def info(message:String, error:Throwable) =  {
   log.info(message, error)
 } 
 
 def warn(message:String, values:Any*) =  {
     log.warn(message, values.map(_.asInstanceOf[Object]).toArray)
 } 
 def warn(message:String, error:Throwable) = { 
   log.warn(message, error)
 } 
 
 def error(message:String, values:Any*) =   {
     log.error(message, values.map(_.asInstanceOf[Object]).toArray)
     Console.println( s"ERROR - SATISFACTION $message ")
 } 

 def error(message:String, error:Throwable) = {
   try {
    log.error(message, error)
    Console.println( s"ERROR - SATISFACTION $message :: ${error.getLocalizedMessage()} ")
    if(error!= null) {
       error.printStackTrace( Console.out)
       error.printStackTrace( System.out)
    }
   } catch {
     case illin : IllegalStateException => {
       System.out.println(" Illegal State Exception while attempting to log !!!! ", illin) 
       illin.printStackTrace(System.out)
       System.out.println(" Original Error was " + message)
       error.printStackTrace(System.out)
     }
   }
 } 

}