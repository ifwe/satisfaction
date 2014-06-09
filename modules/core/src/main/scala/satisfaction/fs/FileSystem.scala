package com.klout
package satisfaction.fs

import java.io.ByteArrayOutputStream


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
   
   def open( pth : Path) : java.io.InputStream
   
   def create( path : Path ) : java.io.OutputStream
   
   def exists( p : Path ) : Boolean
   def isDirectory( p : Path ) : Boolean
   def isFile( p : Path ) : Boolean
   
   
   def getStatus( p : Path ) : FileStatus
   def delete( p : Path )
   
   val BlockSize = 1024*256;

   def readFile( path : Path ) : Array[Byte] = {
     val bufferStream = new ByteArrayOutputStream
     val inStream = open(path)
     transfer( inStream, bufferStream)
     bufferStream.toByteArray
   }
   
   def writeFile( p : Path, buffer : Array[Byte] ) : Unit = {
      create( p).write( buffer)
   }
   
   def transfer( inStream : java.io.InputStream, outStream : java.io.OutputStream) : Long = {
     try {
       val byteArray = new Array[Byte]( BlockSize )
       var nb =0
       var totalBytes = 0
       while ( { nb=inStream.read( byteArray, 0, BlockSize); nb } != -1  ) {
          outStream.write( byteArray, 0, nb) 
          totalBytes += nb
       }
       totalBytes
     } finally {
       inStream.close 
       outStream.close
     }
   }

   def copyToFileSystem( destFS : FileSystem , srcPath : Path, destPath : Path)  = {
      if(! destFS.exists( destPath.parent)) {
        destFS.mkdirs( destPath.parent) 
      }
      val outPath : Path = if( destFS.exists(destPath) ) {
        if( destFS.isDirectory( destPath ) ) {
           destPath / srcPath.name
        } else {
           destFS.delete( destPath)  
           destPath
        }
      } else {
        destPath 
      }
      val inStream =  open( srcPath)
      val outStream = destFS.create( outPath)
      transfer( inStream, outStream)
   }
   
}