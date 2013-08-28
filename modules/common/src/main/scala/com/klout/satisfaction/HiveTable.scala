package com.klout.satisfaction

import hive.ms._
import org.apache.hadoop.hive.ql.metadata._
import collection.JavaConversions._

case class HiveTable(
    dbName: String,
    tblName: String) extends DataOutput {

    private val ms = hive.ms.MetaStore

    def variables = {
        ms.getVariablesForTable(dbName, tblName)
    }

    def exists(w: Witness): Boolean = {
        getDataInstance(w).isDefined
    }

    def getDataInstance(w: Witness): Option[DataInstance] = {
        val partition = getPartition(w)
        println(" partition is " + partition)
        if (partition != null)
            Option(new HiveTablePartition(partition, ms))
        else
            None
    }

    def getPartition(witness: Witness): Partition = {
        try {
            ms.getPartition(dbName, tblName, witness.substitution.raw)
        } catch {
            case e: NoSuchElementException =>
                null
        }
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