package com.klout.satisfaction

import hive.ms._
import org.apache.hadoop.hive.ql.metadata._
import collection.JavaConversions._

/**
 *   Represents a group of partitions
 *   on a table, which might be
 *   partitioned further by a different
 *     column
 */
case class HiveTablePartitionGroup(
    dbName: String,
    tblName: String,
    grouping: Variable[String]) extends DataOutput {

    private val ms = hive.ms.MetaStore

    def variables = {
        Set(grouping)
    }

    def exists(w: Witness): Boolean = {
        getDataInstance(w).isDefined
        
        //// XXX
         ///true
    }

    def getDataInstance(w: Witness): Option[DataInstance] = {
        val tbl = ms.getTableByName(dbName, tblName)
        if (!w.variables.contains(grouping))
            None
        val partMap: Map[String, String] = Map(grouping.name -> w.substitution.get(grouping).get)
        println(s" PART MAP IS $partMap ")
        val hivePartSet = ms.getPartitionSetForTable(tbl, partMap)
        if (hivePartSet.size > 0) {
            Some(new HivePartitionSet(hivePartSet.map(new HiveTablePartition(_)).toSet))
        } else {
            None
        }
    }

}