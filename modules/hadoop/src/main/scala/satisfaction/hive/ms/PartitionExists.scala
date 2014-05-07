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
case class PartitionCreator(
    table : HiveTable 
    ) extends Satisfier with Evidence {
  
    override def name = "PartitionExists"
      
    
    def satisfy(subst: Witness): ExecutionResult = robustly {
        val part = table.ms.addPartition( table.dbName, table.tblName , subst.raw  )    
        true
    }
    
    
    def abort() : ExecutionResult = robustly {
        /// XXX FIXME 
        ///  Drop partition on abort create partition ????
        ///ms.dropPartition( table.dbName, table.tblName )
        true 
    }
    
    /**
     *  
     */
    def exists(w: Witness): Boolean = {
        table.exists(w)
    }

}

object PartitionExists {
  
      def apply( hiveTable : HiveTable )
               (implicit  track : Track) : Goal
                =  {
         val partitionCreator = new PartitionCreator(hiveTable)
         new Goal( name= s" ${hiveTable.tblName} Partition exists",
               satisfier=Some(partitionCreator)
           ).addEvidence(partitionCreator)
      }
  
  
}