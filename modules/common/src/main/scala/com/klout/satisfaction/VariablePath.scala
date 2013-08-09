package com.klout.satisfaction

import hive.ms.Hdfs
import org.apache.hadoop.fs.Path

class VariablePath(pathTemplate: String) extends DataOutput {

    def instanceExists(witness: Witness): Boolean = {
        Hdfs.exists(getPathForWitness(witness))
    }
    def getDataInstance(witness: Witness): DataInstance = {
        new HdfsPath(getPathForWitness(witness))
    }

    def getPathForWitness(witness: Witness): Path = {
        var substPath: String = pathTemplate
        witness.variableValues.foreach{
            case (k, v) =>
                substPath = substPath.replaceAll("${" + k + "}", v)
        }
        println(" Substitution path is " + substPath)
        new Path(substPath)
    }
}