package satisfaction
package hadoop
package hive.ms


import org.joda.time._


/**
 *  DataInstance for HiveTable with no partitions
 *   is a non-partitioned table.
 */
case class NonPartitionedTable(
      hiveTable : HiveTable)
      (implicit val ms : MetaStore) extends DataInstance {
  
    def size: Long = {
       ms.getSpaceUsed(hiveTable.dbName, hiveTable.tblName).toLong /// XXX Return size ??
    }

    def created: DateTime = {
      /// XXX TBD  FIXME
      ////  Add Method to metastore
      null
    }

    def lastAccessed: DateTime = {
      /// XXX TBD FIXME 
      //// Add method to metastore
      null
    }
    
    def exists: Boolean = {
        ms.tableExists( hiveTable.dbName, hiveTable.tblName) 
    }

}