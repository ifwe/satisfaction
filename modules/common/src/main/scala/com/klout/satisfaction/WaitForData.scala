package com.klout.satisfaction

/**
 * Naive implementation for wait for data ...
 *  Can be replaced
 */

object WaitForData extends Satisfier with DataProducing {
  
  override def satisfyGoal(goal: Goal, goalPeriod: Witness) = {
    if(!goal.isInstanceOf[ExternalDataGoal]) 
      throw new IllegalArgumentException(" Goal must be instanceo of ExternalGoal")
     
    val externalGoal : ExternalDataGoal = goal.asInstanceOf[ExternalDataGoal]
    
    while (! externalGoal.externalData.instanceExists( goalPeriod)) {
       Thread.sleep(externalGoal.sleepInterval)
    }
    val instance = externalGoal.externalData.getDataInstance(goalPeriod)
    if( instance.getSize < externalGoal.minimumSize) 
      throw new RuntimeException("Size is too small for data instance " + instance)
    
  }

}
