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
   
   def readFile( path : Path ) : String 
   
   def open( pth : Path) : io.BufferedSource 
   
   def copyToFileSystem( destFS : FileSystem , srcPath : Path, destPath : Path) 
   
   def exists( p : Path ) : Boolean
   def isDirectory( p : Path ) : Boolean
   def isFile( p : Path ) : Boolean
   
   def getStatus( p : Path ) : FileStatus
}