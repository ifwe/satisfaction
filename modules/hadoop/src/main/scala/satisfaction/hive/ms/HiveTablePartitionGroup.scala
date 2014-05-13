package com.klout
package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata._
import collection.JavaConversions._
import satisfaction.fs.FileSystem

/**
 *   Represents a group of partitions
 *   on a table, which might be
 *   partitioned further by a different
 *     column
 *   If at least one partition exists    
 */
case class HiveTablePartitionGroup(
    val dbName: String,
    val tblName: String,
    val grouping: Set[Variable[_]],
    val requiredPartitions :Option[Set[VariableAssignment[_]]] = None)
    ( implicit val ms : MetaStore,
      implicit val hdfs : FileSystem ) extends DataOutput {


    override def variables = {
        grouping
    }

    def exists(w: Witness): Boolean = {
        getDataInstance(w).isDefined
        
        val partSetPossible = getDataInstance(w)
        
    }

    def getDataInstance(w: Witness): Option[DataInstance] = {
        val tbl = ms.getTableByName(dbName, tblName)
        if (!w.variables.contains(grouping))
            None
        val partMap: Map[String, String] = Map(grouping.name -> w.get(grouping).get)
        println(s" PART MAP IS $partMap ")
        val hivePartSet = ms.getPartitionSetForTable(tbl, partMap)
        if (hivePartSet.size > 0) {
            Some(new HivePartitionSet(hivePartSet.map(new HiveTablePartition(_)).toSet))
        } else {
            None
        }
    }

}