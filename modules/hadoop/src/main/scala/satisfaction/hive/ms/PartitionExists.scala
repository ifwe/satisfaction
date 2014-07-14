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
    val table : HiveTable,
    val extraAssignments : Witness = Witness.empty
    )( implicit val track : Track )
      extends Satisfier  with Evidence with Logging {
    var w : Witness = null
  
    override def name = "PartitionExists " + table.toString + " :: "  + extraAssignments.toString
    
      
    override def satisfy(subst: Witness): ExecutionResult = robustly {
        w = subst ++ extraAssignments
        val part = table.addPartition(w)
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
      table.getPartition(w ++ extraAssignments) match {
        case Some(p) => true
        case None => false
      }
    }
    

}

object PartitionExists {
  
      def apply( hiveTable : HiveTable ,
      		     extraAssignments : Witness = Witness.empty )
               (implicit  track : Track) : Goal
                =  {
         val partitionCreator = new PartitionExistsSatisfier(hiveTable, extraAssignments)
         val dataPath = hiveTable.partitionPath
         val goal = new Goal( name= s" Partition Exists ${hiveTable.tblName} $extraAssignments",
               satisfier=Some(partitionCreator),
               variables= hiveTable.variables.filter( ! extraAssignments.variables.contains(_) )
           ).addEvidence( partitionCreator).addWitnessRule( w => { val newW =  w ++ extraAssignments;
               println(s"GGGG New Witness is $newW  extra = $extraAssignments ")
               newW } , DataDependency(dataPath)) 
         goal
      }
  
  
}