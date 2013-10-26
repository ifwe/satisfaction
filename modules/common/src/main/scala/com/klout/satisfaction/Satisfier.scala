package com.klout.satisfaction

import collection.mutable.{ HashMap => MutableHashMap }

trait Satisfier {

    def satisfy(subst: Substitution): Boolean

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
  
  
  def mergeMetrics( other : MetricsCollection ) : Unit = {
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
  }
  
}

/**
 *  A satisfier which gather information about its job run 
 *    can implement this interface 
 */
trait MetricsProducing  {
  
   def jobMetrics  : MetricsCollection 
   
}