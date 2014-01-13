package hive.ms

import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory

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
}

  
case class Hdfs(val fsURI: String) extends satisfaction.fs.FileSystem {
  
  val init = HdfsFactoryInit

    lazy val fs = FileSystem.get(new URI(fsURI), Config.config)
    
    
    override def listFiles( rootPath : Path ) : Seq[FileStatus] = {
        fs.listStatus(rootPath)
    }
    
    override def listFilesRecursively( rootPath : Path ) : Seq[FileStatus] = {
      var fullList : collection.mutable.Buffer[FileStatus] = new ArrayBuffer[FileStatus]
      listFiles( rootPath).foreach( { fs : FileStatus =>
         if( fs.isFile ) {
           fullList += fs
         } else {
           fullList += fs
           fullList ++= listFilesRecursively( fs.getPath)
         }
      })
      fullList
    }
    
    def readFile( path : Path ) : String = {
      io.Source.fromInputStream( fs.open( path) ).getLines.mkString("\n")
    }
    
    
    def globFiles( rootPath : Path ) : Seq[FileStatus] = {
       fs.globStatus(rootPath)
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
    
    

    def exists(path: Path): Boolean = {
        return fs.exists(path)
    }
    
    def open( pth : Path) : io.BufferedSource = {
       io.Source.fromInputStream( fs.open( pth))
    }
    
    def isDirectory( path : Path) : Boolean = {
      fs.getFileStatus(path).isDirectory()
    }

    def markSuccess(path: Path): Unit = {
        val successPath = new Path(path.toUri + "/_SUCCESS")
        if (!fs.exists(successPath)) {
            val writer = fs.create(successPath)
            writer.hsync
            writer.flush
            writer.hflush
            writer.close
        }
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