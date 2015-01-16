package satisfaction
package hadoop.hive

import satisfaction.ProgressCounter
import scala.collection.immutable.List
import _root_.org.apache.hadoop.hive.ql.QueryPlan
import satisfaction.GoalState
import collection.JavaConversions._
import _root_.org.apache.hadoop.hive.ql.exec.mr.MapRedTask
import _root_.org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper
import satisfaction.hadoop.HadoopJobProgress

class HiveProgress( val localDriver : HiveLocalDriver )  extends ProgressCounter {
  
    override def taskName  = {
      /// XXXX
       "Hive Progress Counter"
    }
    
    
    /** 
     *   Estimate from each of the MapRedTasks
     */
    override def progressPercent =  { super.progressPercentFromSubtasks( None) } 
    
    
    /** 
     *   Get the query
     */
    /**
    def queryPlan : QueryPlan = {
       ///localDriver.queryPlan
       ///null
      null
    }
    * 
    */
    
    /**
     *  The subtasks of a given Hive Query, are all the individual 
     *   map reduce tasks found in the query plan
     */
    def subtasks : List[(String,GoalState.State)] = {
      /**
      if( queryPlan == null ) {
         List.empty  
      } else { 
          queryPlan.getRootTasks.filter( _.isInstanceOf[MapRedTask] ).map( tsk => {
             ( tsk.getName, getStateOfMapRedTask(tsk.asInstanceOf[MapRedTask])  ) 
        } ).toList
      }
      * **
      */
      List.empty
    }
    
    private def getStateOfMapRedTask( mrt : MapRedTask) : GoalState.State = {
       if (mrt.started ) {
         if( mrt.done ) {
           /// XXX How to check if task failed ?
        	 GoalState.Success
         } else {
            GoalState.Running 
         }
       } else {
          GoalState.Unstarted 
       }
    }

    /**
     *   Use the HadoopJobExecHelper
     *     to get the running jobs
     *     
     *     XXX Make sure name matches sub-tasks
     */
    def runningSubTasks : Set[ProgressCounter] = {
        ////HadoopJobExecHelper.runningJobs.map( new HadoopJobProgress( _ )).toSet
      Set.empty
    }

    
    /**
     *  measure of units which have completed.
     *  For example Hadoop counters ,
     *   number of query subtasks.
     */
    def progressUnits : MetricsCollection =  {
      /// ??? TODO 
      ////  Should we aggregate counters ????
      //// XXXX Number of steps passed ???
       new MetricsCollection(s"HiveQuery:: " )
    }
      

}