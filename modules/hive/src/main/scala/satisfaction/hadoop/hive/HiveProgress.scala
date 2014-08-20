package satisfaction
package hadoop
package hive

import satisfaction.ProgressCounter
import scala.collection.immutable.List
import org.apache.hadoop.hive.ql.QueryPlan
import satisfaction.GoalState
import collection.JavaConversions._
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper

class HiveProgress(val queryPlan : QueryPlan )  extends ProgressCounter {
  
    override def taskName  = {
        queryPlan.getQueryString()
    }
    
    
    /** 
     *   Estimate from each of the MapRedTasks
     */
    override def progressPercent =  { super.progressPercentFromSubtasks( None) } 
    
    
    /**
     *  The subtasks of a given Hive Query, are all the individual 
     *   map reduce tasks found in the query plan
     */
    def subtasks : List[(String,GoalState.State)] = {
      queryPlan.getRootTasks.filter( _.isInstanceOf[MapRedTask] ).map( tsk => {
        ( tsk.getName, getStateOfMapRedTask(tsk.asInstanceOf[MapRedTask])  )
      } ).toList
      
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
        HadoopJobExecHelper.runningJobs.map( new HadoopJobProgress( _ )).toSet
    }

    
    /**
     *  measure of units which have completed.
     *  For example Hadoop counters ,
     *   number of query subtasks.
     */
    def progressUnits : Set[ProgressUnit] =  {
      /// ??? TODO 
      ////  Should we aggregate counters ????
       Set.empty 
    }
      
             /// XXX chop it done into a smaller string

}