package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata._
import collection.JavaConversions._
import satisfaction.fs.FileSystem

/**
 *   Represents a group of partitions
 *   on a table, which might be
 *   partitioned further by a different
 *     column
 *   If at least one partition exists    
 */
case class HiveTablePartitionGroup(
    val dbName: String,
    val tblName: String,
    val grouping: List[Variable[_]],
    val requiredPartitions :Option[Set[VariableAssignment[_]]] = None)
    ( implicit val ms : MetaStore,
      implicit val hdfs : FileSystem ) extends HiveDataOutput {

    override def toString() = {
      val tblPart = s"HiveTable(${dbName}.${tblName})"
      val groupingPart = grouping.map( _.name).mkString("_")
      val varPart = s"Vars(${groupingPart})"
      val partitionPart = requiredPartitions match {
        case Some(parts) =>{
          val mkVa = parts.map( va =>  s"${va.variable.name}=${va.value}" ).mkString("_")
          s"Partitions(${mkVa})"
        }
        case None => ""
      }
      
      s"${tblPart}_${varPart}_${partitionPart}"
    }

    override def variables : List[Variable[_]] = {
        grouping
    }

    def exists(w: Witness): Boolean = {
        getDataInstance(w).isDefined
        
    }

    def getDataInstance(w: Witness): Option[DataInstance] = {
        val tbl = ms.getTableByName(dbName, tblName)
        if (!w.variables.contains(grouping))
            None
        val partMap: Map[String, String] = grouping.map( { v => { v.name -> w.get(v).get.toString } } ).toMap
        val hivePartSet = ms.getPartitionSetForTable(tbl, partMap)
        if (hivePartSet.size > 0) {
            Some(new HivePartitionSet(hivePartSet.map(new HiveTablePartition(_)).toSet))
        } else {
            None
        }
    }

}