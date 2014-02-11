package com.klout
package satisfaction
package hadoop

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.tools.DistCp
import org.apache.hadoop.util.ToolRunner
import org.joda.time.DateTime
import hdfs._
import fs._
import org.apache.hadoop.mapred.JobClient
import org.apache.hadoop.mapred.JobStatus
import org.apache.hadoop.mapred.RunningJob

class DistCpSatisfier(val src: VariablePath, val dest: VariablePath)( implicit val hdfs : FileSystem , implicit val track: Track) extends Satisfier  {

    var execResult : ExecutionResult = null
    var _distCp : DistCp = null

    @Override
    override def satisfy(projParams: Substitution): ExecutionResult = {
        try {
          
            val srcPath: HdfsPath = src.getDataInstance(new Witness(projParams)).get.asInstanceOf[HdfsPath]
            val destPath: HdfsPath = dest.getDataInstance(new Witness(projParams)).get.asInstanceOf[HdfsPath]
           
            val execName = s"DistCp $srcPath to $destPath"
            execResult = new ExecutionResult( execName, DateTime.now)

            if (destPath.exists) {
                if (srcPath.path.equals(destPath)) {
                    //// If the src path is actually the same as the destpath for some reason,
                    ////  Like the src cluster being the same as the dest cluster...
                    ////  Don't bother copying
                    execResult.isSuccess = true
                    execResult.timeEnded = DateTime.now
                    return execResult
                } else {
                    //// Otherwise, it seems like someone wanted the path to be overwritten
                    //// Delete it first
                    println(s" Deleting existing Path ${destPath.path.toUri.toString}")
                    hdfs.delete(destPath.path)
                }
            }
            println(s"Distcp'ing ${srcPath} to ${destPath.path.toUri} ")

            val args: Array[String] = Array[String](srcPath.toString, destPath.toString)
            val result = distcp(args);
            //// Does DistCp have return codes ??
            println(s" Result of DistCp is $result")
            if (result == 0) {
                Hdfs.markSuccess(hdfs, destPath.path)
                execResult.markSuccess
            } else {
                execResult.markFailure
            }
        } catch {
            case unexpected: Throwable =>
                println(s" Received unexpected error while performing DistCp $unexpected")
                unexpected.printStackTrace(System.out)
                execResult.markUnexpected( unexpected)
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
    
    

    def distcp(args: Array[String]): Int = {
        val job = new JobConf();
        _distCp = new DistCp(job);
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
