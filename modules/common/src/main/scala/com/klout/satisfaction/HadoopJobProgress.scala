package com.klout.satisfaction

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
    @Override
    def progressPercent = {
        hadoopJob.mapProgress*0.45 + hadoopJob.reduceProgress*0.45 + 
        hadoopJob.setupProgress*0.05 + hadoopJob.cleanupProgress*0.05
    }
    
    
    /**
     *  Include a bunch of counters, so that more accurate progress may be measured ..
     */
    @Override
    override def progressUnits : Set[ProgressUnit] = {
      /// XXX TODO.. filter out only the counters we care about 
      /// XXX Make scala wrapper around hadoop counters ...
       hadoopJob.getCounters.write( new java.io.DataOutputStream(System.out))
       val counters = hadoopJob.getCounters
       Set( ProgressUnit( counters.getCounter( MAP_INPUT_RECORDS) , "Map Input Records") ,
            ProgressUnit( counters.getCounter( MAP_OUTPUT_RECORDS), "Map Output Records") ,
            ProgressUnit( counters.getCounter( MAP_INPUT_BYTES), "Map Input Bytes") ,
            ProgressUnit( counters.getCounter( MAP_OUTPUT_BYTES), "Map Output Bytes") ,
            ProgressUnit( counters.getCounter( REDUCE_INPUT_RECORDS), "Reduce Input Records") ,
            ProgressUnit( counters.getCounter( REDUCE_OUTPUT_RECORDS), "Reduce Output Records") ,
            ProgressUnit( counters.getCounter( REDUCE_INPUT_GROUPS), "Reduce Input Groups") ,
            ProgressUnit( counters.getCounter( CPU_MILLISECONDS), "CPU Milliseconds") 
           )
           
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