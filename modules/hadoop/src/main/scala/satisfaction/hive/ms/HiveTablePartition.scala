package com.klout
package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata.Partition
import hive.ms._
import org.joda.time._

case class HiveTablePartition(
    part: Partition) extends DataInstance {

    implicit val ms: MetaStore = MetaStore

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

    def exists: Boolean = {
        /// XXX 
        true
    }

    def lastModifiedBy: String = {
        getMetaData("last_modified_by").getOrElse(null)
    }

    def getMetaData(key: String): Option[String] = {
        ms.getPartitionMetaData(part).get(key)
    }

}