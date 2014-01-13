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
       if( uri.endsWith("/") || that.startsWith("/")) {
           new Path( uri + that)
       } else {
          new Path( uri + "/" + that)
       }
    }
  
    def toUri : java.net.URI = {
      new java.net.URI( pathString)
    }
    
    override def toString : String = {
       pathString 
    }
    
    
}
