package com.klout.satisfaction

import hive.ms._
import org.apache.hadoop.hive.ql.metadata._

case class HiveTable(
    dbName: String,
    tblName: String) extends DataOutput {

    private val ms = hive.ms.MetaStore

    def variables = Set.empty

    def exists(witness: Witness): Boolean = {
        getPartition(witness) != null
    }

    def getDataInstance(witness: Witness): Option[DataInstance] = {
        val partition = getPartition(witness)
        Some(new HiveTablePartition(partition))
    }

    def getPartition(witness: Witness): Partition = {
        val tbl = ms.getTableByName(dbName, tblName)
        val partCols = tbl.getPartCols()
        //// Place logic in MetaStore ???
        var partSpec = List[String]()
        for (i <- 0 until partCols.size - 1) {
            partSpec ++ witness.params.get(partCols.get(i).getName())
        }
        print(" PartSpec = " + partSpec)

        ms.getPartition(dbName, tblName, partSpec)
    }
}