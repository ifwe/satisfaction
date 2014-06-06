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
case class PartitionExistsSatisfier(
    val table : HiveTable
    )( implicit val track : Track )
      extends Satisfier  with Evidence with Logging {
    var w : Witness = null
  
    override def name = "PartitionExists " + table.toString
      
    override def satisfy(subst: Witness): ExecutionResult = robustly {
        w = subst
        val part = table.addPartition(subst)
        info(" Added Partition " + part )
        true
    }
    
    override def abort() : ExecutionResult = robustly {
      if(w != null) {
       table.getPartition(w) match {
        case Some(p) => 
          p.drop
        case None =>
       }
      }
      true
    }
    
    override def exists(w: Witness): Boolean = {
      table.getPartition(w) match {
        case Some(p) => true
        case None => false
      }
    }
    

}

object PartitionExists {
  
      def apply( hiveTable : HiveTable )
               (implicit  track : Track) : Goal
                =  {
         val partitionCreator = new PartitionExistsSatisfier(hiveTable)
         val dataPath = hiveTable.partitionPath
         val goal = new Goal( name= s" ${hiveTable.tblName} Partition exists",
               satisfier=Some(partitionCreator),
               variables=hiveTable.variables
           ).addEvidence( partitionCreator).addDataDependency(dataPath)
         goal
      }
  
  
}