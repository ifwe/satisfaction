package com.klout
package satisfaction
package hadoop
package hdfs

import java.net.URI
import fs._
import org.apache.hadoop.fs.{Path => ApachePath}
import org.apache.hadoop.fs.{FileStatus => ApacheFileStatus}
import org.apache.hadoop.fs.{FileSystem => ApacheFileSystem}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.joda.time.DateTime
import satisfaction.hadoop.Config._
import org.apache.hadoop.fs.FSDataOutputStream

/**
 *
 */


object HdfsFactoryInit {
         val fsClass = classOf[org.apache.hadoop.fs.FileSystem] 
         val meths = fsClass.getDeclaredMethods()
         val loadFSMeth = fsClass.getDeclaredMethod("loadFileSystems")
         loadFSMeth.setAccessible(true)
         loadFSMeth.invoke(null)
         
         val fsFactory : FsUrlStreamHandlerFactory  =  {
            
              val newFactory = new org.apache.hadoop.fs.FsUrlStreamHandlerFactory();
              try { 
                  java.net.URL.setURLStreamHandlerFactory(newFactory);
                  newFactory
              }catch { 
                case  e :  Throwable =>
                  if( e.getMessage.contains("factory already defined")) {
                     println(" Ignoring factory already defined error") 
                  }
                  fsFactory
              }
         }
         
    implicit def ApachePath2Path( ap : ApachePath) : Path = {
       new Path( ap.toUri.toString )
    }
    
    implicit def Path2ApachePath( p : Path) : ApachePath = {
      new ApachePath( p.toString ) 
    }
}

case class HdfsFStat( apacheFileStatus : ApacheFileStatus ) extends satisfaction.fs.FileStatus {
  
    override def getSize : Long = {
	  apacheFileStatus.getLen
    }
    
    override def isDirectory : Boolean = {
	  apacheFileStatus.isDirectory
    }
    
    override def isFile : Boolean = {
	  apacheFileStatus.isFile
    }
    
    override def getPath : Path = {
	  new Path(apacheFileStatus.getPath.toUri.toString)
    }
    
    def lastAccessed : DateTime = {
      new DateTime(apacheFileStatus.getAccessTime)
    }
    def created : DateTime = {
      new DateTime(apacheFileStatus.getModificationTime)
    }
    
  
}
object HdfsFStat {
    implicit def ApacheFileStatus2HdfsFS( apacheFileStatus : ApacheFileStatus ) : HdfsFStat = {
       new HdfsFStat( apacheFileStatus ) 
    }
    
}

object ApachePath {
   implicit def ApachePath2Path( ap : ApachePath) : Path = {
       new Path( ap.toUri.toString )
    }
    
    implicit def Path2ApachePath( p : Path) : ApachePath = {
      new ApachePath( p.toString ) 
    }
   
  
    implicit def FileSystem2ApacheFileSystem( fs : FileSystem ) : ApacheFileSystem = {
       ApacheFileSystem.get( fs.uri , Config.config) 
    }
}
  
case class Hdfs(val fsURI: String) extends satisfaction.fs.FileSystem {
  
   import ApachePath._
   import HdfsFStat._
  
  val init = HdfsFactoryInit

    lazy val fs = ApacheFileSystem.get(new URI(fsURI), Config.config)
    
   
    override def uri : java.net.URI = {
    	fs.getUri()
   }
   
    override def listFiles( rootPath : Path ) : Seq[FileStatus] = {
        fs.listStatus(  rootPath ).toSeq.map( afs => { afs : FileStatus } )
    }
    
    
    @Override
    override def listFilesRecursively( rootPath : Path ) : Seq[FileStatus] = {
      var fullList : collection.mutable.Buffer[FileStatus] = new ArrayBuffer[FileStatus]
      listFiles( rootPath).foreach( { fs : FileStatus =>
         if( !fs.isDirectory ) {
           fullList += fs
         } else {
           fullList += fs
           fullList ++= listFilesRecursively( fs.getPath)
         }
      })
      fullList
    }
    
    override def readFile( path : Path ) : String = {
      io.Source.fromInputStream( fs.open( path) ).getLines.mkString("\n")
    }
    
    
    def globFiles( rootPath : Path ) : Seq[FileStatus] = {
       fs.globStatus(rootPath).toSeq.map( afs => { afs : FileStatus })
    }
    
    def globFilesRecursively( rootPath : Path ) : Seq[FileStatus] = {
      var fullList : collection.mutable.Buffer[FileStatus] = new ArrayBuffer[FileStatus]
      listFiles( rootPath).foreach( { fs : FileStatus =>
         if( fs.isFile ) {
           fullList += fs
         } else {
           fullList += fs
           fullList ++= globFilesRecursively( fs.getPath)
         }
      })
      fullList
    }
    
    override def exists(path: Path): Boolean = {
        return fs.exists(path)
    }
    
    override def getStatus( path : Path) : FileStatus =  {
	   return fs.getFileStatus( path)
    }
    
    override def open( pth : Path) : io.BufferedSource = {
       io.Source.fromInputStream( fs.open( pth))
    }
    
    override def isDirectory( path : Path) : Boolean = {
      fs.getFileStatus(path).isDirectory()
    }
      
    override def isFile( path : Path) : Boolean = {
      fs.getFileStatus(path).isFile()
    }
    
     
   override def copyToFileSystem( destFS : FileSystem , srcPath : Path, destPath : Path) = {
     val apacheDestFS : ApacheFileSystem = destFS
     
   }
   
   override def delete( path : Path ) = {
     fs.delete( path, true) 
   }
   
   override def create( path : Path ) : java.io.OutputStream = {
       val hdfsStream : FSDataOutputStream = fs.create( path) 
       
       val closingStream = new java.io.OutputStream {
          override def write( b : Int ) = {
            hdfsStream.write(b) 
          }
          override def write( bArr : Array[Byte], off : Int, len : Int) = {
            hdfsStream.write( bArr, off,len) 
          }
          override def flush() = {
              hdfsStream.hsync
              hdfsStream.hflush
              hdfsStream.flush
          }
          
          override def close() = {
              hdfsStream.hsync
              hdfsStream.flush
              hdfsStream.hflush
              hdfsStream.close 
          }  
       }
       
       return closingStream
   }
   
    

    def getSpaceUsed(path: Path): Long = {
        var totalLen: Long = 0
        if (fs.exists(path)) {
            val status = fs.getFileStatus(path)
            if (status.isDirectory()) {
                val fsArr = fs.listFiles(status.getPath(), true)
                while (fsArr.hasNext()) {
                    val lfs = fsArr.next
                    totalLen += lfs.getLen()
                }
                return totalLen
            } else {
                status.getLen()
            }
        } else {
            0
        }
    }

}


object Hdfs extends Hdfs("hdfs://jobs-dev-hnn:8020") {
  
   def markSuccess(fs : FileSystem, path: Path): Unit = {
        val successPath = new Path(path.toString + "/_SUCCESS")
        if (!fs.exists(successPath)) {
            val writer = fs.create(successPath)
            writer.flush
            writer.close
        }
    }


    def rounded(dbl: Double): String = {
        (((dbl * 100).toInt) / 100.0).toString
    }
    //  Return a string which translates from a long 
    //   to a string including Mb or Gb or Tb
    def prettyPrintSize(lngSize: Long): String = {
        val kb: Double = lngSize.toDouble / 1024.0
        if (kb < 10) {
            return lngSize + " bytes"
        } else {
            val mb = kb / 1024.0
            if (mb < 1.0) {
                return rounded(kb) + "K"
            } else {
                val gb = mb / 1024.0
                if (gb < 1.0) {
                    return rounded(mb) + " Mb"
                } else {
                    val tb = gb / 1024.0
                    if (tb < 1.0) {
                        return rounded(gb) + " Gb"
                    } else {
                        return rounded(tb) + " Tb"
                    }
                }
            }
        }
    }

}