package com.klout
package satisfaction
package hadoop
package hive.ms


import satisfaction._
import satisfaction.Satisfier
import scala.collection.JavaConversions._


/**
 *  PartitionExists is similar to HiveTable DataDependency,
 *    but creates the necessary partitions if they don't exist.
 */
case class PartitionExists(
    table : HiveTable 
    )( implicit val ms : MetaStore)  extends Satisfier with Evidence {
  
    override def name = "PartitionExists"
      
    
    def satisfy(subst: Substitution): ExecutionResult = robustly {
        val part = ms.addPartition( table.dbName, table.tblName , subst.raw  )    
        true
    }
    
    
    def abort() : ExecutionResult = {
      null 
    }
    
    
    
    /**
     *  
     */
    def exists(w: Witness): Boolean = {
        table.exists(w)
    }

}