package com.klout.satisfaction


/**
 *  counters for units which may measure amout of progress 
 *   which has occurred so for,
 *  For example, number of seconds elapsed, number of mappers completed,
 *    number of steps in a Query accomplished, number of units in oozie flow
 *   
 */
case class ProgressUnit( val amount : Double, val units : String)  {
        
}
    
/**
 * Some satisfiers may be able to get insight into how much progress
 *   has been done so for, so that predictions can be made as to
 *     job completion 
 *     
 *  For Hadoop and Hive jobs, this can involve accessing the job client,
 *   or multiple job clients ...
 */
trait ProgressCounter {
    
    /**
     *  Rough estimate of total progress of the task so far.
     */
    def progressPercent : Double
    
    /**
     *  measure of units which have
     */
    def progressUnits : Set[ProgressUnit]
  
}


trait Progressable {

   def progressCounter : ProgressCounter
   
}