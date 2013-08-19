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

    def exists(params: ParamMap): Boolean = {
        getDataInstance(params).isDefined
    }

    def getDataInstance(params: ParamMap): Option[DataInstance] = {
        val partition = getPartition(params)
        Option(new HiveTablePartition(partition))
    }

    def getPartition(witness: Witness): Partition = {

        ms.getPartition(dbName, tblName, witness.params)
    }
        /**
    def getPartition(params: ParamMap): Partition = {
        val tbl = ms.getTableByName(dbName, tblName)
        val partCols = tbl.getPartCols()
        //// Place logic in MetaStore ???
        var partSpec = List[String]()
        for (i <- 0 until partCols.size - 1) {
            partSpec ++ params.raw.get(partCols.get(i).getName())
        }
        print(" PartSpec = " + partSpec)

        ms.getPartition(dbName, tblName, partSpec)
>>>>>>> master
    }
    * **
    */
}