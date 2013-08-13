package com.klout.satisfaction

import hive.ms.Hdfs
import org.apache.hadoop.fs.Path

case class VariablePath(pathTemplate: String) extends DataOutput {

    def variables = Set.empty

    def exists(witness: Witness): Boolean = {
        Hdfs.exists(getPathForWitness(witness))
    }
    def getDataInstance(witness: Witness): Option[DataInstance] = {
        Some(new HdfsPath(getPathForWitness(witness)))
    }

    def getPathForWitness(witness: Witness): Path = {
        var substPath: String = pathTemplate
        witness.params.foreach{
            case (k, v) =>
                substPath = substPath.replaceAll("${" + k + "}", v)
        }
        println(" Substitution path is " + substPath)
        new Path(substPath)
    }
}