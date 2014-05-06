package com.klout
package satisfaction
package hadoop

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.tools.DistCp
import org.apache.hadoop.util.ToolRunner
import org.joda.time.DateTime
import hdfs.Hdfs
import hdfs.HdfsImplicits
import hdfs.HdfsPath
import hdfs.VariablePath
import fs.FileSystem
import fs.Path
import org.apache.hadoop.mapred.JobClient
import org.apache.hadoop.mapred.JobStatus
import org.apache.hadoop.mapred.RunningJob
import org.apache.hadoop.tools.DistCpOptions
import org.apache.hadoop.fs.{Path => ApachePath}

class DistCpSatisfier(val src: VariablePath, val dest: VariablePath)( implicit val hdfs : FileSystem , implicit val track: Track) extends Satisfier  {

    override def name = s"DistCp $src to $dest "

    var execResult : ExecutionResult = null
    var _distCp : DistCp = null

    @Override
    override def satisfy(projParams: Witness): ExecutionResult =  robustly {
      true
    }

    def XXXsatisfy(projParams: Witness) :ExecutionResult =  robustly {
          
            val srcPath: HdfsPath = src.getDataInstance(projParams).get.asInstanceOf[HdfsPath]
            val destPath: HdfsPath = dest.getDataInstance(projParams).get.asInstanceOf[HdfsPath]
           

            if (destPath.exists) {
                if (srcPath.path.equals(destPath)) {
                    true
                } else {
                    //// Otherwise, it seems like someone wanted the path to be overwritten
                    //// Delete it first
                    println(s" Deleting existing Path ${destPath.path.toUri.toString}")
                    hdfs.delete(destPath.path)
                    println(s"Distcp'ing ${srcPath} to ${destPath.path.toUri} ")

            		val result = distcp(srcPath.path, destPath.path);
                   	//// Does DistCp have return codes ??
                    println(s" Result of DistCp is $result")
                    result == 0
                }
            } else {
            println(s"Distcp'ing ${srcPath} to ${destPath.path.toUri} ")

            val result = distcp(srcPath.path, destPath.path);
            //// Does DistCp have return codes ??
            println(s" Result of DistCp is $result")
            result == 0
            }
    } 

    
    /**
     * Determine if the job is our DistCp job
     */
    def isDistCpJob( js: JobStatus , jc: JobClient) : Boolean = {
      /// XXX 
       val checkJob: RunningJob =  jc.getJob( js.getJobID)
       //// Figure out proper Job name
       checkJob.getJobName.toLowerCase.contains("distcp")
    }
    
    @Override 
    override def abort() : ExecutionResult = {
      if(_distCp != null) {
         val jobClient = new JobClient(_distCp.getConf() )
         val allJobs = jobClient.getAllJobs
        allJobs.filter( isDistCpJob( _, jobClient )) foreach( js => {
            if( js.getRunState() == JobStatus.RUNNING) {
               println(s"Killing job ${js.getJobId}  ")
               val checkJob: RunningJob =  jobClient.getJob( js.getJobID)
               checkJob.killJob
               return execResult.markFailure 
            }
        } )
        ///TODO XXX Fix DistCp abort
      
      }
      
       execResult.markFailure
    }
    
    

    def distcp(src : Path, dest : Path): Int = {
        val job = new JobConf();


        /// Why won't my implicits get invoked ???
        val apacheSrc : ApachePath = HdfsImplicits.Path2ApachePath(src);
        val apacheDest : ApachePath = HdfsImplicits.Path2ApachePath(dest);
        ///val apacheDest : ApachePath = dest;

        val opts = new DistCpOptions(apacheSrc,apacheDest)
        
        _distCp = new DistCp(job, opts);
        val args = new Array[String](0)

        ToolRunner.run(_distCp, args);
    }

}

object DistCpGoal {
    /// XXX Is this the correct implicit scope???
    implicit val hdfs : FileSystem  = Hdfs.default
   
    def apply(goalName: String, src: VariablePath, dest: VariablePath )
        (implicit  track: Track): Goal = {
        val srcVars = src.variables
        val destVars = dest.variables
        ////  Add check  that  vars are the same 
        if (srcVars.size != destVars.size) {
            throw new IllegalArgumentException("Paths must have same variables")
        }
        new Goal(
            name = goalName,
            satisfier = Some(new DistCpSatisfier(src, dest)),
            variables = srcVars,
            dependencies = Set.empty,
            evidence = Set(dest)
        )
    }

}
