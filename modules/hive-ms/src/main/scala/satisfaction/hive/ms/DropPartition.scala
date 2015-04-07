package satisfaction
package hadoop
package hive.ms


import satisfaction._
import satisfaction.Satisfier
import satisfaction.SatisfierFactory
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException
import org.apache.hadoop.hive.ql.metadata.HiveException


/**
 *  DropPartition provides a way delete 
 *    older partitions in a table, 
 *    in order to free up space for 
 *    newer data
 */
case class DropPartitionSatisfier(
    val table : HiveTable,
    val extraAssignments : Witness = Witness.empty
    )( implicit val track : Track )
      extends Satisfier  with Evidence with Logging {
    var w : Witness = null
  
    override def name = "DropPartition_" + table.toString + "("  + extraAssignments.toString + ")"
    
      
    override def satisfy(subst: Witness): ExecutionResult = robustly {
        w = subst ++ extraAssignments
        try { 
            table.dropPartition( w)
        } catch {
          case hiveException : HiveException => {
            hiveException.getCause match {
              case doesntExist : UnknownPartitionException => {
                 warn(s" Partition on table ${table.dbName}::${table.tblName} doesnt exist for witness $w ; Possible race condition with other Track ??")
              }
              case unexpected : Throwable =>
                 throw unexpected
            }
          }
          case doesntExist : UnknownPartitionException => {
             warn(s" Partition on table ${table.dbName}::${table.tblName} doesnt exist for witness $w ; Possible race condition with other Track ??")
          }
          case unexpected : Throwable =>
             throw unexpected
        }

        true
    }
    
    override def abort() : ExecutionResult = robustly {
      true
    }
    
    
    override def exists(w: Witness): Boolean = {
      table.getPartition(w ++ extraAssignments) match {
        case None => true 
        case Some(p) => false
      }
    }
    

}

object DropPartition {
  
      def apply( hiveTable : HiveTable ,
      		     extraAssignments : Witness = Witness.empty )
               (implicit  track : Track) : Goal
                =  {
         val partitionDropper = new DropPartitionSatisfier(hiveTable, extraAssignments)
         new Goal( name= s" DropPartition ${hiveTable.tblName} $extraAssignments",
               satisfierFactory=Goal.SatisfierFactory(partitionDropper),
               variables= hiveTable.variables.filter( ! extraAssignments.variables.contains(_) )
           )
      }
  
  
}