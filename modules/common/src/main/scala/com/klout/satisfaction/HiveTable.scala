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

    def exists(witness: Witness): Boolean = {
        getPartition(witness) != null
    }

    def getDataInstance(witness: Witness): Option[DataInstance] = {
        val partition = getPartition(witness)
        Some(new HiveTablePartition(partition))
    }

    def getPartition(witness: Witness): Partition = {

        ms.getPartition(dbName, tblName, witness.params)
    }
}