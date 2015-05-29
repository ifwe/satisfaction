package satisfaction.fs

import java.io._
import org.joda.time.DateTime

/**
 *  Simple FileSystem, for accessing local disk.
 *  Mostly for testing...
 *  
 *  XXX Add unit tests for local file operations 
 */
case class LocalFileSystem( val nfs : java.nio.file.FileSystem  = java.nio.file.FileSystems.getDefault) extends FileSystem {
  
  def this()  = {
     this( java.nio.file.FileSystems.getDefault) 
  }

  case class LocalFStatus( file : java.io.File ) extends FileStatus {
      
    override def size : Long = {
      file.length
    }
    override def isDirectory : Boolean  = {
      file.isDirectory
    }
    
    override def isFile : Boolean = {
      file.isFile 
    }
    
    override def path : Path = {
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
     return new java.net.URI( s"file:///")
   }
   
   override def listFiles( p : Path ) : Seq[FileStatus] = {
       val file :File = (p)
       val lf = file.listFiles
       if( lf == null ) {
         Seq.empty 
       } else {
         lf.map(f => { 
           new LocalFStatus(f)  
           } ).toSeq
       }
   }
   
   
   override def listFilesRecursively( p : Path ) : Seq[FileStatus] = {
     listFiles( (p) ).map( fs =>  { 
         if( fs.isFile ) {
           Seq( fs)
         } else if( fs.isDirectory ) {
           listFilesRecursively( fs.path ) ++ Seq(fs)
         } else {
           Seq.empty
         }
       } ).flatten
   }
   
   override def globFiles( p: Path) : Seq[FileStatus] = {
      val pathMatcher =  nfs.getPathMatcher( p.toString ) 
       null
   }
   
   override def mkdirs( p : Path ) : Boolean = {
     (p).mkdirs
   }
   
   
   override def open( path : Path) : java.io.InputStream = {
     new FileInputStream((path))
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

object LocalFileSystem extends LocalFileSystem( java.nio.file.FileSystems.getDefault)  {
   def currentDirectory : Path = {
      new Path( System.getProperty("user.dir"))
   } 
   
   def relativePath( p : Path) : Path = {
      currentDirectory / p 
   }
}
