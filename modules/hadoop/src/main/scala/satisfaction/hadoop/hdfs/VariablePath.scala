package satisfaction
package hadoop
package hdfs

import fs._

case class VariablePath(pathTemplate: String, checkMarked : Boolean , minSize : Long )(implicit val hdfs : FileSystem, val track : Track)
      extends DataOutput with Logging {

    def variables = {
        Substituter.findVariablesInString(pathTemplate).map (Variable(_)).distinct
    }

    def exists(witness: Witness): Boolean = {
        getPathForWitness(witness) match {
            case None       => false
            case Some(path) => {
               if(hdfs.exists(path) ) {
                  val hdfsPath = new HdfsPath( path)
                  if( checkMarked) {
                    if ( ! hdfsPath.isMarkedCompleted ) {
                      return false
                    }
                  }
                  if( minSize > 0 ) {
                    if ( hdfsPath.size < minSize) {
                      return false
                    }
                  }
                  true
               } else {
                 false
               }
            }
        }
    }

    def getDataInstance(witness: Witness): Option[DataInstance] = {
        getPathForWitness(witness) match {
            case None       => None
            case Some(path) => Some(new HdfsPath(path))
        }
    }

    def getPathForWitness(witness: Witness): Option[Path] = {
        val fullSubstituter = track.getTrackProperties(witness)
        var substPath = Substituter.substitute(pathTemplate, fullSubstituter)
        substPath match {
            case Left(missingVars) =>
                warn(" Missing vars " + missingVars.mkString(",") + " ; no Path for witness")
                 None
            case Right(substitutedPath) =>
                info(s" Reified path is  ${substitutedPath} ")
                Some(new Path(substitutedPath))
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
      
      new VariablePath( sb.toString(), false, 0 )
    }

    def apply( strPath : Path )
         ( implicit hdfs: FileSystem, track : Track): VariablePath = {
      new VariablePath( strPath.toString, false, 0 )
    }

    def apply( str : String )
         ( implicit hdfs: FileSystem, track : Track): VariablePath = {
      new VariablePath( str, false, 0 )
    }
  
}