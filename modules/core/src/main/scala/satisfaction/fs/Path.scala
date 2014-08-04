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
    
    
    def parent : Path = {
       val pathArr  = pathString.split(Separator)
       new Path(pathArr.take( pathArr.length -1 ).mkString( Separator + "" ))
    }
    
}

