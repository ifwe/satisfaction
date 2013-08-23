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
    grouping: VariableAssignment[Any]) extends DataOutput {

    private val ms = hive.ms.MetaStore

    def variables = {
        ms.getVariablesForTable(dbName, tblName)
    }

    def exists(w: Witness): Boolean = {
        getDataInstance(w).isDefined
    }

    def getDataInstance(w: Witness): Option[DataInstance] = {
        ////val partition = getPartition(w)
        ///Option(new HiveTablePartition(partition))
        null
    }

    def getPartitions(witness: Witness): Set[Partition] = {
        val partNames = ms.getPartitionNamesForTable(dbName, tblName)

        ///ms.getPartition(dbName, tblName, partSpec)

        ///ms.getPartition(dbName, tblName, witness.params.raw)
        null
    }
    /**
     * def getPartition(params: ParamMap): Partition = {
     * val tbl = ms.getTableByName(dbName, tblName)
     * val partCols = tbl.getPartCols()
     * //// Place logic in MetaStore ???
     * var partSpec = List[String]()
     * for (i <- 0 until partCols.size - 1) {
     * partSpec ++ params.raw.get(partCols.get(i).getName())
     * }
     * print(" PartSpec = " + partSpec)
     *
     * ms.getPartition(dbName, tblName, partSpec)
     * >>>>>>> master
     * }
     * **
     */
}