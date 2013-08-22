package com.klout.satisfaction

import org.apache.hadoop.fs.Path

class DistCpSatisfier(val src: VariablePath, val dest: VariablePath) extends Satisfier {
    val config = hive.ms.Config.config

    def satisfy(params: ParamMap): Boolean = {
        try {

            val srcPath = src.getDataInstance(new Witness(params)).get.asInstanceOf[HdfsPath].path.toUri.toString
            val destPath = dest.getDataInstance(new Witness(params)).get.asInstanceOf[HdfsPath].path.toUri.toString

            val args: Array[String] = Array[String](srcPath, destPath)
            org.apache.hadoop.tools.DistCp.main(args);
            true
        } catch {
            case unexpected: Throwable =>
                false
        }
    }

}
