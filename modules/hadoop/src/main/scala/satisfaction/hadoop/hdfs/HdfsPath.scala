package satisfaction
package hadoop
package hdfs

import org.joda.time.DateTime

import fs._

case class HdfsPath(val path: Path)(implicit val hdfs : FileSystem ) extends DataInstance with Markable {

    lazy val status: FileStatus = hdfs.getStatus(path)


    def size: Long = {
      status.size
    }

    def created: DateTime = {
        status.created
    }

    def lastAccessed: DateTime = {
      status.lastAccessed
    }


    override def toString: String = {
        path.toUri.toString
    }
    
    def markCompleted : Unit = {
       val fd = hdfs.create( path / "_SUCCESS" )
       fd.write( Array[Byte]() )
       fd.flush
       fd.close
    }

    def markIncomplete : Unit = {
       val sucPath = path / "_SUCCESS"
       if( hdfs.exists(sucPath)) {
         hdfs.delete( sucPath) 
       }
    }

    def isMarkedCompleted : Boolean = {
         hdfs.exists( path / "_SUCCESS" )
    }
    
    
  
}
