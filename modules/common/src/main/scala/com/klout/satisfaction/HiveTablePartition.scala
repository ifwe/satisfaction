package com.klout.satisfaction

import org.apache.hadoop.hive.ql.metadata.Partition
import hive.ms._
import org.joda.time._

class HiveTablePartition(
    part: Partition) extends DataInstance {

    def size: Long = {
        MetaStore.getPartitionSize(part)
    }

    def created: DateTime = {

        new DateTime(MetaStore.getPartitionMetaData(part).get("created").get.toLong * 1000)
    }

    def lastAccessed: DateTime = {
        new DateTime(part.getLastAccessTime() * 1000)
    }

}