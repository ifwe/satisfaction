package com.klout
package satisfaction
package hadoop
package hdfs

import fs._

case class VariablePath(pathTemplate: String)(implicit val hdfs : FileSystem, val track : Track)
      extends DataOutput {

    def variables = {
        /// XXX interpret certain variables as dates
        Substituter.findVariablesInString(pathTemplate).map (Variable(_))
    }

    def exists(witness: Witness): Boolean = {
        getPathForWitness(witness) match {
            case None       => false
            case Some(path) => hdfs.exists(path)
        }
    }

    def getDataInstance(witness: Witness): Option[DataInstance] = {
        getPathForWitness(witness) match {
            case None       => None
            case Some(path) => Some(new HdfsPath(path))
        }
    }

    def getPathForWitness(witness: Witness): Option[Path] = {
        /// XXX Who is going to set the Track ????
        val fullSubstituter = track.getTrackProperties(witness)
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

object VariablePath {
  
    /**
     *  Return Variable Path
     */
    def apply( basePath : Path , vars : List[Variable[_]], expandVars : Set[Variable[_]] = Set.empty)
         ( implicit hdfs: FileSystem, track : Track): VariablePath = {
      val sb = new StringBuilder
      sb.append( basePath.toString)
      vars.foreach( v => {
          sb.append("/")
          if(expandVars.contains(v)) {
            sb.append(v.name)
            sb.append("=")
            sb.append()
          } 
          sb.append("${")
          sb.append(v.name)
          sb.append("}")
      })
      
      new VariablePath( sb.toString() )
    }
  
}