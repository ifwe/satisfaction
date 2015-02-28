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
/// XXX Better name ??? HivePartitionGroup vs HivePartitinSet ????
case class HivePartitionSet(
    val partitionSet: Set[HiveTablePartition])
    extends DataInstance with Markable {

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


    def lastModifiedBy: String = {
        partitionSet.toSeq.head.lastModifiedBy
    }
    
    /**
     *  Mark that the producer of this
     *   DataInstance fully completed .
     */
    def markCompleted : Unit = {
      partitionSet.foreach( _.markCompleted)
    }
    
    def markIncomplete : Unit = {
        partitionSet.foreach( _.markIncomplete ) 
    }
    
    /**
     *  Check that the Data instance has been Marked completed,
     *    according to the test of the markable.
     */
    def isMarkedCompleted : Boolean = {
       partitionSet.forall( _.isMarkedCompleted ) 
    }
  

}