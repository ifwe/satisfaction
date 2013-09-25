package com.klout.satisfaction

import hive.ms.Hdfs
import org.apache.hadoop.fs.Path

case class VariablePath(pathTemplate: String) extends DataOutput with ProjectOriented {

    def variables = {
        /// XXX interpret certain variables as dates
        Substituter.findVariablesInString(pathTemplate).map (Variable(_))
    }

    def exists(witness: Witness): Boolean = {
        getPathForWitness(witness) match {
            case None       => false
            case Some(path) => Hdfs.exists(path)
        }
    }

    def getDataInstance(witness: Witness): Option[DataInstance] = {
        getPathForWitness(witness) match {
            case None       => None
            case Some(path) => Some(new HdfsPath(path))
        }
    }

    def getPathForWitness(witness: Witness): Option[Path] = {
        val fullSubstituter = getProjectProperties(witness.substitution)
        var substPath = Substituter.substitute(pathTemplate, fullSubstituter)
        substPath match {
            case Left(missingVars) =>
                println(" Missing vars " + missingVars.mkString(",") + " ; no Path for witness")
                return None
            case Right(substitutedPath) =>
                println(s" Reified path is  ${substitutedPath} ")
                return Some(new Path(substitutedPath))
        }
    }
}