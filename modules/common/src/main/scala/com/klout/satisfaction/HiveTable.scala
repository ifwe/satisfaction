package com.klout.satisfaction

import hive.ms._
import org.apache.hadoop.hive.ql.metadata._

class HiveTable(
    dbName: String,
    tblName: String) extends DataOutput {
    private val ms = hive.ms.MetaStore;

    def instanceExists(witness: Witness): Boolean = {
        getPartition(witness) != null
    }

    def getDataInstance(witness: Witness): DataInstance = {
        val partition = getPartition(witness)
        new HiveTablePartition(partition)
    }

    def getPartition(witness: Witness): Partition = {
        val tbl = ms.getTableByName(dbName, tblName)
        val partCols = tbl.getPartCols()
        //// Place logic in MetaStore ???
        var partSpec = List[String]()
        for (i <- 0 until partCols.size - 1) {
            partSpec ++ witness.variableValues.get(partCols.get(i).getName())
        }
        print(" PartSpec = " + partSpec)

        ms.getPartition(dbName, tblName, partSpec)
    }
}