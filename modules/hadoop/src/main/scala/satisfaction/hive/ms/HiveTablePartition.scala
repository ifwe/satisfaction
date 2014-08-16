package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata.Partition
import hive.ms._
import org.joda.time._
import fs._
import hdfs.HdfsImplicits._

case class HiveTablePartition(
    part: Partition)
    (implicit val ms : MetaStore,
              val hdfs : FileSystem ) extends DataInstance  with Markable {


    def size: Long = {
        ms.getPartitionSize(part)
    }

    def created: DateTime = {
        val createdMetaData = getMetaData("created")
        createdMetaData match {
            case Some(secCount) =>
                msDateTime(secCount.toLong)
            case None =>
                getMetaData("transient_lastDdlTime") match {
                    case Some(fallbackTime) => msDateTime(fallbackTime.toLong)
                    case None               => null
                }
        }
    }

    /// SIC ... is that OK ???
    def lastAccessedTime: DateTime = lastModifiedTime

    def lastModifiedTime: DateTime = {
        val createdMetaData = getMetaData("last_modified_time")
        println(" created MetaData is  " + createdMetaData)
        createdMetaData match {
            case Some(secCount) =>
                msDateTime(secCount.toLong)
            case None => null
        }
    }

    def msDateTime(msLong: Long): DateTime = {
        println(" MS LONG = " + msLong)
        new DateTime(msLong * 1000)
    }
    

    def lastAccessed: DateTime = {
        msDateTime(part.getLastAccessTime)
    }

    
    def path : fs.Path = {
       part.getDataLocation
    }

    def lastModifiedBy: String = {
        getMetaData("last_modified_by").getOrElse(null)
    }

    def getMetaData(key: String): Option[String] = {
        ms.getPartitionMetaData(part, key)
    }
    
    def setMetaData( key: String, md : String ) : Unit = {
       ms.setPartitionMetaData(part, key, md) 
    }
    
    def drop : Unit = {
      /// XXX expose on metastore
    }
    
   /**
     *  Mark that the producer of this
     *   DataInstance fully completed .
     */
    def markCompleted : Unit = {
       setMetaData("isComplete" , "true")  
    }
    
    def markIncomplete : Unit = {
       setMetaData("isComplete" , "false")  
    } 

    /**
     *  Check that the Data instance has been Marked completed,
     *    according to the test of the markable.
     */
    def isMarkedCompleted : Boolean = {
      getMetaData("isComplete") match {
        case Some( check) => {
           check.toBoolean
        }
        case None => false
      }
    }
  

}