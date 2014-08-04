package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata.Partition
import hive.ms._
import org.joda.time._

/**
 *  Data instance for a set of Hive Partitions
 *   which were probably created by a Hive query
 *   with dynamic partitioning
 */
case class HivePartitionSet(
    val partitionSet: Set[HiveTablePartition])
    extends DataInstance {

    def size: Long = {
        partitionSet.map(_.size).sum
    }

    def created: DateTime = {
        partitionSet.toSeq.head.created
    }

    /// SIC ... is that OK ???
    def lastAccessedTime: DateTime = lastModifiedTime

    def lastModifiedTime: DateTime = {
        partitionSet.toSeq.head.lastModifiedTime
    }

    def lastAccessed: DateTime = {
        partitionSet.toSeq.head.lastModifiedTime
    }

    def exists: Boolean = {
        partitionSet.forall( _.exists)
    }

    def lastModifiedBy: String = {
        partitionSet.toSeq.head.lastModifiedBy
    }

}