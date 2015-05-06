package satisfaction
package hadoop 

import scala.collection.immutable.Set
import org.apache.hadoop.mapred.RunningJob
import org.apache.hadoop.mapred.Task.Counter._
import collection.JavaConversions
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapred.JobClient
import org.apache.hadoop.conf.Configuration


/**
 *  Use a Hadoop JobClient to track progress  
 *    of a Hadoop job
 */

class HadoopJobProgress( val hadoopJob : RunningJob ) extends ProgressCounter {
  
    /**
     *  Use the name of the the job 
     */
    override def taskName = {
        hadoopJob.getJobName 
    } 
    
    
    /**
     *  For single map-reduce job, say there are no 
     *   sub tasks 
     *   XXX Define subtasks as Map/Sort/Reduce
     */
    override def subtasks = List.empty
    
    
    /**
     *   TODO -- define in terms of Map/Reduce
     */
    override def runningSubTasks = Set.empty
    
    /**
     *  Define the total progress as the 
     *    mapper progress*0.45 + reducer progress *0.45
     *    + startup progress*0.05 + cleanup progres * 0.05
     *  Obviously not quite right, but good first pass
     *  
     *  Startup progress and cleanup normally a small amount 
     *    of total progress, but we should factor it in, so 
     *    that we don't say 0% before mappers have started,
     *    and we don't say 100% right after reducers have completed.
     */
    override def progressPercent = {
        hadoopJob.mapProgress*0.45 + hadoopJob.reduceProgress*0.45 + 
        hadoopJob.setupProgress*0.05 + hadoopJob.cleanupProgress*0.05
    }
    
    
    /**
     *  Include a bunch of counters, so that more accurate progress may be measured ..
     */
    override def progressUnits : MetricsCollection= {
      /// XXX TODO.. filter out only the counters we care about 
      /// XXX Make scala wrapper around hadoop counters ...
       hadoopJob.getCounters.write( new java.io.DataOutputStream(System.out))
       
       ///
      /// XXX Add Task Completion Events ...
       
       val counters = hadoopJob.getCounters
       val mc = new MetricsCollection("Hadoop Counters")
       mc.setMetric( "Map Input Records",  counters.getCounter( MAP_INPUT_RECORDS))
       mc.setMetric( "Map Output Records",  counters.getCounter( MAP_OUTPUT_RECORDS))
       mc.setMetric( "Map Input Bytes",  counters.getCounter( MAP_INPUT_BYTES))
       mc.setMetric( "Map Output Bytes",  counters.getCounter( MAP_OUTPUT_BYTES))
       mc.setMetric( "Reduce Input Records",  counters.getCounter( REDUCE_INPUT_RECORDS))
       mc.setMetric( "Reduce Output Records",  counters.getCounter( REDUCE_OUTPUT_RECORDS))
       mc.setMetric( "Reduce Input Groups",  counters.getCounter( REDUCE_INPUT_GROUPS))
       mc.setMetric( "CPU Milliseconds",  counters.getCounter( CPU_MILLISECONDS))
       
       mc
    }

}


object HadoopJobProgress {
  
  def apply(config : Configuration ,  jobIDStr : String) : HadoopJobProgress = {
    val jobId = JobID.forName( jobIDStr)
    val jobClient = new JobClient( config)
    val hadoopJob = jobClient.getJob( jobId)
    new HadoopJobProgress( hadoopJob)
  }
  
}