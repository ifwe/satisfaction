package com.klout.satisfaction

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.tools.DistCp
import org.apache.hadoop.util.ToolRunner

class DistCpSatisfier(val src: VariablePath, val dest: VariablePath) extends Satisfier with ProjectOriented {
    val config = hive.ms.Config.config

    implicit lazy val hdfs = hive.ms.Hdfs

    def satisfy(projParams: Substitution): Boolean = {
        try {

            val srcPath: HdfsPath = src.getDataInstance(new Witness(projParams)).get.asInstanceOf[HdfsPath]
            val destPath: HdfsPath = dest.getDataInstance(new Witness(projParams)).get.asInstanceOf[HdfsPath]

            if (destPath.exists) {
                if (srcPath.path.equals(destPath)) {
                    //// If the src path is actually the same as the destpath for some reason,
                    ////  Like the src cluster being the same as the dest cluster...
                    ////  Don't bother copying
                    return true
                } else {
                    //// Otherwise, it seems like someone wanted the path to be overwritten
                    //// Delete it first
                    println(s" Deleting existing Path ${destPath.path.toUri.toString}")
                    hdfs.fs.delete(destPath.path, true)
                }
            }
            println(s"Distcp'ing ${srcPath} to ${destPath.path.toUri} ")

            val args: Array[String] = Array[String](srcPath.toString, destPath.toString)
            val result = distcp(args);
            //// Does DistCp have return codes ??
            println(s" Result of DistCp is $result")
            if (result == 0) {
                hdfs.markSuccess(destPath.path)
                return true
            } else {
                return false
            }
        } catch {
            case unexpected: Throwable =>
                println(s" Received unexpected error while performing DistCp $unexpected")
                unexpected.printStackTrace(System.out)
                false
        }
    }

    def distcp(args: Array[String]): Int = {
        val job = new JobConf();
        val distcp = new DistCp(job);
        ToolRunner.run(distcp, args);
    }

}

object DistCpGoal {
    def apply(goalName: String, src: VariablePath, dest: VariablePath): Goal = {
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
            evidence = Set(dest)
        )
    }

}
