package satisfaction
package hadoop
package hive.ms


import satisfaction._
import satisfaction.Satisfier
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.ql.metadata.HiveException


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
        val part = try { 
           table.addPartition(w)
        } catch {
          //// In a race condition, if another track wants to check
          //// for the same partition at the same time, 
          ////  An "AlreadyExistsException" may be thrown
          case hiveExc  : HiveException => {
             hiveExc.getCause match {
               case alreadyExists : AlreadyExistsException => {
                 warn(s" Partition on table ${table.dbName}::${table.tblName} already exists for witness $w ; Possible race condition with other Track ??")
                 table.getPartition(w).get
               }
               case unexpected : Throwable => 
                   throw unexpected
             }
          }
          case alreadyExists : AlreadyExistsException => {
             warn(s" Partition on table ${table.dbName}::${table.tblName} already exists for witness $w ; Possible race condition with other Track ??")
             table.getPartition(w).get
          }
          case unexpected : Throwable =>
             throw unexpected
        }

        part.markCompleted
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
        case Some(p) => {
          info(s" Found Partition for Table ${table.dbName}::${table.tblName} $w")
          if(p.isMarkedCompleted) {
             true
          } else {
            warn(s" Found Partition for Table ${table.dbName}::${table.tblName} $w ; But it is not marked Complete")
            false 
          }
        }
        case None =>  {
          info(s" Could not find Partition for Table ${table.dbName}::${table.tblName} $w")
          false
        }
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
               satisfierFactory=Goal.SatisfierFactory(partitionCreator),
               variables= hiveTable.variables.filter( ! extraAssignments.variables.contains(_) )
           ).addEvidence( partitionCreator).addWitnessRule( w => {  w ++ extraAssignments }
                , DataDependency(dataPath)) 
         goal
      }
  
  
}