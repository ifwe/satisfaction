package satisfaction.fs

/**
 *  Abstraction for paths
 *  to shield us from Hadoop dependencies
 */


object PathImplicits {
  
    implicit def URI2Path( value : java.net.URI) : Path = {
    	new Path( value.toString())
    }
    
    implicit def String2Path( str : String ) : Path = {
      new Path( str)
    }
    
}

case class Path(val  pathString : String ) {
  
     val Separator = '/'
     
     def /(  that : String) : Path= {
       val uri = toUri.toString
       val prefix = if(uri.endsWith("/")) {
         uri.substring( 0, uri.length -1) 
       } else {
         uri
       }
       val extension = if( that.startsWith("/")) {
         that.substring(1)
       } else {
         that
       }
       new Path( prefix + Separator  + extension)
    }
     def /(  that : Path) : Path= {
       /( that.pathString)
     }
  
    def toUri : java.net.URI = {
      new java.net.URI( pathString)
    }
    
    
    def name : String = {
      pathString.split(Separator).last
    }
    
    override def toString : String = {
       pathString 
    }
    
    def split = {
      pathString.split("/")
    }
    
    def parent : Path = {
       val pathArr  = pathString.split(Separator)
       new Path(pathArr.take( pathArr.length -1 ).mkString( Separator + "" ))
    }
    
}

/**
 *   Walk up the parents of the path
 */
class PathIterator( val bottomPath : Path ) extends Iterator[Path] {
  private var _currentPath = bottomPath

  override def hasNext: Boolean = {
      _currentPath.pathString.contains("/")  /// Not quite right, could have URI schemes, but good for now 
  }
  
  override def next(): Path = {
     _currentPath = _currentPath.parent 
     _currentPath
  }
}

class PathSequence( val bottomPath : Path ) extends Seq[Path] {
  override def apply(idx: Int): Path = {
     val parts = bottomPath.split
     var buildPath : Path =  Path(parts(0))
     for( i <- 1 to (parts.size - idx) )  yield {
        buildPath = buildPath / parts(i)
        buildPath
     }
     buildPath
  }

  override  def iterator: Iterator[Path] = {
     new PathIterator(bottomPath)
  }
 
  override def length: Int = {
    bottomPath.split.size     
  }

}

object Path {
    type PathFilter = (FileStatus) => Boolean
  
}

