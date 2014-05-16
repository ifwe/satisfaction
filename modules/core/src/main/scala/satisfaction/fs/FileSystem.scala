package com.klout
package satisfaction.fs


/**
 *  Want to mock out FileSystem and MetaStore into traits
 *    So that we can stub out implementations without  
 *   accessing HDFS directly,
 *   
 *   and we could have potential other non-hadoop implementations,
 *    and 
 */

trait FileSystem {

   def uri : java.net.URI
   def listFiles( p : Path ) : Seq[FileStatus]
   def listFilesRecursively( p : Path ) : Seq[FileStatus]
   
   def mkdirs( p : Path ) : Boolean
   
   def readFile( path : Path ) : String 
   
   def open( pth : Path) : io.BufferedSource 
   
   def create( path : Path ) : java.io.OutputStream
   
   def copyToFileSystem( destFS : FileSystem , srcPath : Path, destPath : Path)  = {
      println(s" Copyting from $srcPath to $destPath ")
      val str = readFile(srcPath)
      println(" STRI = " + str)
      if(! destFS.exists( destPath.parent)) {
        destFS.mkdirs( destPath.parent) 
      }
      val outStream = destFS.create( destPath)
      outStream.write( str.getBytes)
   }
   
   def exists( p : Path ) : Boolean
   def isDirectory( p : Path ) : Boolean
   def isFile( p : Path ) : Boolean
   
   
   def getStatus( p : Path ) : FileStatus
   def delete( p : Path )
}