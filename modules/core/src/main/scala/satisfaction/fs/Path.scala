package com.klout
package satisfaction.fs

/**
 *  Abstraction for paths ,
 *  to shield us from Hadoop
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
       new Path( prefix + "/" + extension)
    }
  
    def toUri : java.net.URI = {
      new java.net.URI( pathString)
    }
    
    
    def name : String = {
      pathString.split("/").last
    }
    
    override def toString : String = {
       pathString 
    }
    
    
    
}
