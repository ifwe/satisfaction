package com.klout
package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata._
import collection.JavaConversions._
import fs._
import hdfs.Hdfs

case class HiveTable(
    dbName: String,
    tblName: String) extends DataOutput {

    implicit val ms = MetaStore
    implicit val hdfs = Hdfs
    
    val checkSuccessFile = true

    def variables = {
        ms.getVariablesForTable(dbName, tblName)
    }

    def exists(w: Witness): Boolean = {
      val partitionOpt = getPartition( w)
      if(partitionOpt.isDefined ) {
        if( checkSuccessFile) {
        	val partition = partitionOpt.get
        	println(s" PARTITION = $partition")
        	println(s" PARTITION = $partition PART PATH = ${partition.getPartitionPath}")
            val successPath = new  Path(partition.getPartitionPath + "/_SUCCESS")
        	hdfs.exists( successPath)
        } else{
          true
        } 
      } else {
        false
      }
    }

    def getDataInstance(w: Witness): Option[DataInstance] = {
        val partition = getPartition(w)
        partition match {
          case Some(part) => Some(new HiveTablePartition(part))
          case None => None 
        }
    }

    def getPartition(witness: Witness): Option[Partition] = {
        try {
            
            val tblWitness = witness.filter( variables)
            println( "variables for table is " + variables  +  " ; Witness variables = " + witness.variables)
            println(s" TableWitness = $tblWitness == regular witness = $witness")
            val part = ms.getPartition(dbName, tblName, tblWitness.substitution.raw)
            if( part != null) {
              Some(part)
            } else {
              None
            }
        } catch {
            case e: NoSuchElementException =>
              None
        }
        
    }
}