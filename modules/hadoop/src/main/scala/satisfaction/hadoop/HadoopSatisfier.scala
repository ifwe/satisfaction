package com.klout
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
   
    
    val satisfier = new HadoopSatisfier[KeyIn,ValIn,MapKeyOut,MapValOut,KeyOut,ValOut](
            name, mapper, reducer, partitionerOptions, inputs, output)
    
     val variables = output.variables
            
      new Goal( name, Some(satisfier), variables, dependencies = Set.empty, Set(output) ) 
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
  
  override def jobMetrics : MetricsCollection  = {
    _hadoopMetrics
  }
  
  var startTime : DateTime = null
  
  override def satisfy(subst: Substitution): ExecutionResult = {
    
    
    
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
    
    
    val jobClient = new JobClient( jobConf)
    val runningJob = jobClient.submitJob(jobConf)
    
    _hadoopProgress = new HadoopJobProgress( runningJob)   
   
    runningJob.waitForCompletion()
    _hadoopMetrics = getMetricsFromCounters( runningJob.getCounters)
    getExecResultFromJob(runningJob)
  }
  
  
  def getExecResultFromJob( job : RunningJob ) : ExecutionResult = {
     val execResult = new ExecutionResult(job.getJobID, new DateTime( job.getJobStatus.getStartTime()))
     job.getJobStatus.getRunState match {
       case JobStatus.SUCCEEDED =>
        execResult.isSuccess  = true
      case JobStatus.FAILED  =>
        execResult.isSuccess  = false
        execResult.errorMessage = job.getJobStatus.getFailureInfo
      case JobStatus.KILLED  =>
        execResult.isSuccess  = false
        execResult.errorMessage = job.getJobStatus.getFailureInfo
     }
     execResult.timeEnded = new DateTime()
     execResult.metrics.mergeMetrics( getMetricsFromCounters( job.getCounters ))
     
     execResult
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