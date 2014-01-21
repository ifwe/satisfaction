package com.klout
package satisfaction
package hadoop

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.tools.DistCp
import org.apache.hadoop.util.ToolRunner
import org.joda.time.DateTime
import hdfs._
import fs._

class DistCpSatisfier(val src: VariablePath, val dest: VariablePath, implicit val hdfs : FileSystem ) extends Satisfier with TrackOriented {

    var execResult : ExecutionResult = null

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
    
    

    def distcp(args: Array[String]): Int = {
        val job = new JobConf();
        val distcp = new DistCp(job);
        ToolRunner.run(distcp, args);
    }

}

object DistCpGoal {
    implicit val hdfs : FileSystem  = Hdfs.default
   
    def apply(goalName: String, src: VariablePath, dest: VariablePath ): Goal = {
        val srcVars = src.variables
        val destVars = dest.variables
        ////  Add check  that  vars are the same 
        if (srcVars.size != destVars.size) {
            throw new IllegalArgumentException("Paths must have same variables")
        }
        new Goal(
            name = goalName,
            satisfier = Some(new DistCpSatisfier(src, dest, hdfs)),
            variables = srcVars,
            evidence = Set(dest)
        )
    }

}
