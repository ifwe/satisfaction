package com.klout.satisfaction

import hive.ms.Hdfs
import org.apache.hadoop.fs.Path

case class VariablePath(pathTemplate: String) extends DataOutput {

    def variables = Set.empty

    def exists(params: ParamMap): Boolean = {
        Hdfs.exists(getPathForParamMap(params))
    }
    def getDataInstance(params: ParamMap): Option[DataInstance] = {
        Some(new HdfsPath(getPathForParamMap(params)))
    }

    def getPathForParamMap(params: ParamMap): Path = {
        val substPath = (pathTemplate /: params.raw) {
            case (path, (k, v)) =>
                path.replaceAll("${" + k + "}", v)
        }
        println(" Substitution path is " + substPath)
        new Path(substPath)
    }
}