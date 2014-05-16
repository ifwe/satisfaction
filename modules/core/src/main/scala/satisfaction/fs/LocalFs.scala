
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
case class LocalFileSystem() extends FileSystem {
  
  case class LocalFStatus( file : java.io.File ) extends FileStatus {
      
    override def getSize : Long = {
      file.getSize 
    }
    override def isDirectory : Boolean  = {
      file.isDirectory
    }
    
    override def isFile : Boolean = {
      file.isFile 
    }
    
    override def getPath : Path = {
      new Path( file.getPath)
    }
    
    override def lastAccessed : DateTime = {
       new DateTime(file.lastModified)
    }
    
    override def created : DateTime = {
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
     /// 
     return new java.net.URI( s"file:///")
   }
   
   /**
   def appendPath( p : Path) : Path = {
     new Path( basePath + "/" +  p.toUri.getPath)
   } 
   * 
   */
     
   override def listFiles( p : Path ) : Seq[FileStatus] = {
       val file :File = (p)
       System.out.println( " File is " + file)
       val lf = file.listFiles
       if( lf == null ) {
         Seq.empty 
       } else {
         lf.map(f => { new LocalFStatus(f)  } ).toSeq
       }
   }
   
   
   override def listFilesRecursively( p : Path ) : Seq[FileStatus] = {
     listFiles( (p) ).map( fs =>  { 
       System.out.println( " Recursive file is " + fs );
         if( fs.isFile ) {
           Seq( fs)
         } else if( fs.isDirectory ) {
           listFilesRecursively( fs.getPath ) ++ Seq(fs)
         } else {
           Seq.empty
         }
       } ).flatten
   }
   
   override def mkdirs( p : Path ) : Boolean = {
     (p).mkdirs
   }
   
   override def readFile( path : Path ) : String = {
     scala.io.Source.fromFile( (path)).mkString
   }
   
   override def open( path : Path) : io.BufferedSource = {
      scala.io.Source.fromFile((path)) 
   }
   
   override def create( path : Path ) : java.io.OutputStream = {
      new FileOutputStream((path))
   }
   
   override def exists( p : Path ) : Boolean = {
     (p).exists 
   }
   
   override def isDirectory( p : Path ) : Boolean = {
     (p).isDirectory
   }
   
   
   override def isFile( p : Path ) : Boolean = {
     (p).isFile
   }
   
   override def getStatus( p : Path ) : FileStatus = {
     val f : File = (p)
      f 
   }
   
   override def delete( p : Path ) =  {
     /// XXX handle return value
     (p).delete 
   }
   
   
   def setExecutable( p: Path, flag: Boolean = true ) = {
     (p).setExecutable( flag)
   }

}

object LocalFileSystem extends LocalFileSystem  {
   def currentDirectory : Path = {
      new Path( System.getProperty("user.dir"))
   } 
}
