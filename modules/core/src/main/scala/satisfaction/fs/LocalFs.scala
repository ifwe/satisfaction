package com.klout
package satisfaction.fs

import java.io._
import org.joda.time.DateTime

/**
 *  Simple FileSystem, for accessing local disk.
 *  Mostly for testing...
 *  
 *  XXX Add unit tests for local file operations 
 */
case class LocalFs( val basePath : String) extends FileSystem {
  
  case class LocalFStatus( file : java.io.File ) extends FileStatus {
      
    override def getSize : Long = {
      file.getSize 
    }
    def isDirectory : Boolean  = {
      file.isDirectory
    }
    def isFile : Boolean = {
      file.isFile 
    }
    
    def getPath : Path = {
      new Path( file.getPath)
    }
    
    def lastAccessed : DateTime = {
       new DateTime(file.lastModified)
    }
    
    def created : DateTime = {
      /// XXX not correct
       lastAccessed
    }
    
  }
  
   implicit def File2FileStatus( file : java.io.File ) : FileStatus = {
      new LocalFStatus(file) 
   }
   
   implicit def Path2File( path : Path) : java.io.File = {
     new File( path.toUri.getPath )
   }
   
   implicit def File2Path( file : java.io.File) : Path = {
       new Path( file.getPath) 
   }
   
  
   override def uri : java.net.URI = {
     return new java.net.URI( s"file://$basePath")
   }
   
   def appendPath( p : Path) : Path = {
     new Path(basePath + "/" +  p.pathString)
   } 
     
   override def listFiles( p : Path ) : Seq[FileStatus] = {
       val file :File = appendPath(p)
       file.listFiles.map(f => { new LocalFStatus(f)  } ).toSeq
   }
   
   
   override def listFilesRecursively( p : Path ) : Seq[FileStatus] = {
     listFiles( appendPath(p) ).map( fs =>  { 
         if( fs.isFile ) {
           Seq( fs)
         } else if( fs.isDirectory ) {
           listFilesRecursively( fs.getPath )
         } else {
           Seq.empty
         }
       } ).flatten
   }
   
   override def readFile( path : Path ) : String = {
     scala.io.Source.fromFile( appendPath(path)).toString
   }
   
   override def open( path : Path) : io.BufferedSource = {
      scala.io.Source.fromFile(appendPath(path)) 
   }
   
   override def create( path : Path ) : java.io.OutputStream = {
      new FileOutputStream(appendPath(path))
   }
   
   /**
    *  XXX Do we need this ????
    *   Needed to copy to from HDFS from localHDFS 
    */
   override def copyToFileSystem( destFS : FileSystem , srcPath : Path, destPath : Path) = {
      val str = readFile(srcPath)
      val outStream = destFS.create( destPath)
      outStream.write( str.getBytes)
   }  
   
   override def exists( p : Path ) : Boolean = {
     appendPath(p).exists 
   }
   
   override def isDirectory( p : Path ) : Boolean = {
     appendPath(p).isDirectory
   }
   
   
   override def isFile( p : Path ) : Boolean = {
     appendPath(p).isFile
   }
   
   override def getStatus( p : Path ) : FileStatus = {
     val f : File = appendPath(p)
      f 
   }
   
   override def delete( p : Path ) =  {
     /// XXX handle return value
     appendPath(p).delete 
   }

}