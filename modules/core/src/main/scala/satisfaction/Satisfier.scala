package com.klout
package satisfaction

import collection.mutable.{ HashMap => MutableHashMap }
import org.joda.time.DateTime
///import com.typesafe.scalalogging.Logging

trait Satisfier {
  
    /**
     *  Some unique identifier to describe the process being run
     */
    def name : String

    /**
     *  Make sure the Goal is satisfied for the specified Witness
     *  
     *  @returns Result of execution
     */
    def satisfy(witness: Witness): ExecutionResult
    
    /**
     *  If possible, abort the job
     */
    def abort() : ExecutionResult
    
    /**
      *  Provide a simple template for Satisfiers to return and ExecutionResult
      * 
      *  Satisfier implementations can simply say
      *   def satisfy( witness : Substitition ) = robustly {
      *       if(doTheThing() == success) {
      *         true
      *       } else {
      *         false
      *       }
      *   }
      *  
      **/
    
    def robustly ( f: => Boolean ) : ExecutionResult = {
           RobustRun( name , f)
    }
}

/**
 *  Create a Function as an object, 
 *   to handle "Running robustly", 
 *    catching errors , and building up the 
 *    ExecutionResult object correctly.
 */
object RobustRun extends Logging {
  
      def apply ( name: String,  func : =>  Boolean ) : ExecutionResult =   {
            val execResult  = new ExecutionResult(name, DateTime.now)
            try {
               val result = func
               if( result) {
                 execResult.markSuccess
               } else {
                 execResult.markFailure
               }
            } catch {
              case unexpected : Throwable => {
                error( s"Error while running satisfier $name ; ${unexpected.getMessage} ", unexpected)
                execResult.markUnexpected(unexpected)
            } 
          }
        }
}


/**
 *  General Structure for holding generated metrics for job runs
 *   as key,value pairs.
 *   
 *   Metrics for the sub-divided tasks are included as well
 */
case class MetricsCollection( val collectionName : String  ) {
  
  val metrics : collection.mutable.Map[String,Any] = new MutableHashMap[String,Any]
  val subMetrics : collection.mutable.Map[String,MetricsCollection] = new collection.mutable.LinkedHashMap[String,MetricsCollection]
  
  def setMetric( metric : String, metVal : Any ) = {
      metrics.put( metric, metVal)  
  }
  
  def incrMetric( metric : String, numVal : Long) : Number = {
     metrics.get( metric)  match {
       case Some(oldVal) =>
         val oldNum : Long = oldVal.asInstanceOf[Long]
         val newVal : Long = numVal + oldNum
         metrics.put( metric, newVal)
         newVal 
       case None =>
         metrics.put( metric, numVal)
         numVal
     }
  }
  
  
  def mergeMetrics( other : MetricsCollection ) : MetricsCollection = {
    other.metrics.foreach { case(k,v) => {
        metrics.put( k, v) 
       } 
    }
    other.subMetrics.foreach{ case(name,mc) => { 
         subMetrics.get( name) match {
           case Some(thisMc) =>
             thisMc.mergeMetrics(mc)
           case None =>
             subMetrics.put( name,mc) 
         }
      }
    }
    this
  }
  
}

/**
 *  A satisfier which gather information about its job run 
 *    can implement this interface 
 */
trait MetricsProducing  {
  
   def jobMetrics  : MetricsCollection 
   
}

