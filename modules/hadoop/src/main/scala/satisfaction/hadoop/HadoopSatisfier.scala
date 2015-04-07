package satisfaction
package hadoop

import org.apache.hadoop.mapred.Mapper
import org.apache.hadoop.mapred.Reducer
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobClient
import org.apache.hadoop.mapred.JobStatus
import org.apache.hadoop.mapred.RunningJob
import org.joda.time.DateTime
import org.apache.hadoop.mapred.Counters
import scala.collection.JavaConversions._


object HadoopGoal {
 
  def apply[KeyIn,ValIn,MapKeyOut,MapValOut,KeyOut,ValOut]( 
      name  : String,
      mapper : Class[_<:Mapper[KeyIn,ValIn,MapKeyOut,MapValOut]],
      reducer : Option[Class[_<:Reducer[MapKeyOut,MapValOut,KeyOut,ValOut]]],
      partitionerOptions : String,
      inputs : Set[DataOutput],
      output : DataOutput )(implicit track : Track) : Goal = {
   
    
    val satisfierFactory : SatisfierFactory = { w => { Some( new HadoopSatisfier[KeyIn,ValIn,MapKeyOut,MapValOut,KeyOut,ValOut](
            name, mapper, reducer, partitionerOptions, inputs, output)) } }
    
     val variables = output.variables
            
      new Goal( name, satisfierFactory, variables, dependencies = Set.empty, Set(output) ) 
  }
  
}

/**
 *  Satisfier for running plain vanilla Hadoop jobs 
 */
case class HadoopSatisfier[KeyIn,ValIn,MapKeyOut,MapValOut,KeyOut,ValOut]( name : String,
        mapper : Class[_<:Mapper[KeyIn,ValIn,MapKeyOut,MapValOut]], 
        reducer : Option[Class[_<:Reducer[MapKeyOut,MapValOut,KeyOut,ValOut]]],
        partitionerOptions : String,
        inputs : Set[DataOutput],
        output :  DataOutput
        )/** (
            implicit val mko:Manifest[MapKeyOut], implicit val mvo:Manifest[MapValOut],
            implicit val ko:Manifest[KeyOut], implicit val vo:Manifest[ValOut]
            ) **/
        extends Satisfier with Progressable with MetricsProducing {
  

  private var _hadoopProgress : HadoopJobProgress = null
  
  override def progressCounter = _hadoopProgress
  
  private var _hadoopMetrics : MetricsCollection = null
  private var _jobClient : JobClient = null
  private var _runningJob : RunningJob = null
  
  override def jobMetrics : MetricsCollection  = {
    _hadoopMetrics
  }
  
  var startTime : DateTime = null
  
  override def satisfy(subst: Witness): ExecutionResult = {
    
    
    
    val jobConf = new JobConf
    jobConf.setMapperClass(mapper)
    if( reducer.isDefined) {
       jobConf.setReducerClass( reducer.get)
    }
    
    jobConf.setJobName( name)
    
    
    /// XXX Use typetags or manifest to set clases ...
    ///jobConf.setOutputKeyClass( ko.erasure)
    ///jobConf.setOutputValueClass( vo.erasure)
    
    
    ///jobConf.setMapOutputKeyClass( mko.erasure)
    ///jobConf.setMapOutputValueClass( mvo.erasure)
    ///jobConf.setPartitionerClass(classOf[KeyFieldPartitioner])
    jobConf.setKeyFieldPartitionerOptions( partitionerOptions)
    
    
    _jobClient = new JobClient( jobConf)
    _runningJob = _jobClient.submitJob(jobConf)
    
    _hadoopProgress = new HadoopJobProgress( _runningJob)   
   
    _runningJob.waitForCompletion()
    _hadoopMetrics = getMetricsFromCounters( _runningJob.getCounters)
    getExecResultFromJob(_runningJob)
  }
  
  
  def getExecResultFromJob( job : RunningJob ) : ExecutionResult = {
     val execResult = new ExecutionResult(job.getJobID, new DateTime( job.getJobStatus.getStartTime()))
     job.getJobStatus.getRunState match {
       case JobStatus.SUCCEEDED =>
         execResult.markSuccess
      case JobStatus.FAILED  =>
         execResult.markFailure( job.getJobStatus().getFailureInfo())
      case JobStatus.KILLED  =>
        /// XXX Is there GoalState Killed???
        //// Or do we want to  mark somehow ??
         execResult.markFailure( job.getJobStatus.getFailureInfo())
     }
     execResult.metrics.mergeMetrics( getMetricsFromCounters( job.getCounters ))
     
     execResult
  }
  
  override def abort() : ExecutionResult = {
    _runningJob.killJob();
    getExecResultFromJob(_runningJob)
  }
  
  
  def getMetricsFromCounters( counters : Counters ) : MetricsCollection = {
    val metrics  = new MetricsCollection(name)
    counters.iterator.foreach{ group => {
      val groupMetrics = new MetricsCollection( group.getName)
      group.iterator.foreach { counter => {
    	  groupMetrics.setMetric( counter.getDisplayName, counter.getCounter )
      }}
      metrics.mergeMetrics( groupMetrics)
    } }
    metrics
  }
  
  
}