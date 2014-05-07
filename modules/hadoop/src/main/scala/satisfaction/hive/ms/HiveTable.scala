package com.klout
package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.fs.{Path => ApachePath}
import collection.JavaConversions._
import fs._
import hdfs.Hdfs
import hdfs.HdfsPath
import hdfs.HdfsFactoryInit
import hdfs.HdfsImplicits._

/**
 *  Implies a table with Partitions
 */
case class HiveTable (
    dbName: String,
    tblName: String,
    isPartitioned: Boolean = true)
    (implicit val ms : MetaStore,
     implicit val hdfs : FileSystem) extends DataOutput  {

    
    val checkSuccessFile = true

    def variables = {
      if(isPartitioned)
        ms.getVariablesForTable(dbName, tblName)
      else 
        Set.empty
    }

    override def exists(w: Witness): Boolean = {
       if(isPartitioned) {
           partitionExists( w)
       } else {
         /// XXX Unit test this ..
          ms.getTableByName(dbName, tblName) != null
       }
    }
    
    def partitionExists(w: Witness): Boolean = {
      val partitionOpt = getPartition( w)
      if(partitionOpt.isDefined ) {
        if( checkSuccessFile) {
        	val partition = partitionOpt.get
        	println(s" PARTITION = $partition")
        	val partPath : Path = partition.getPartitionPath
        	if( hdfs.exists( partPath)) {
        	  /// Check metadata to see if table has a min partition size 
        	   true 
        	} else {
        	  false
        	}
        } else{
          true
        } 
      } else {
        false
      }
    }

    def getDataInstance(w: Witness): Option[DataInstance] = {
      if( isPartitioned ) {
        val partition = getPartition(w)
        partition match {
          case Some(part) => Some(new HiveTablePartition(part))
          case None => None 
        }
      } else {
         Some(new NonPartitionedTable( this))   
       }
    }

    def getPartition(witness: Witness): Option[Partition] = {
        try {
            
            val tblWitness = witness.filter( variables)
            println( "variables for table is " + variables  +  " ; Witness variables = " + witness.variables)
            println(s" TableWitness = $tblWitness == regular witness = $witness")
            val part = ms.getPartition(dbName, tblName, tblWitness.raw)
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