package satisfaction
package hadoop
package hive.ms


import org.joda.time._


/**
 *  DataInstance for HiveTable with no partitions
 *   is a non-partitioned table.
 */
case class NonPartitionedTable(
      val hiveTable : HiveTable)
      (implicit val ms : MetaStore) extends DataInstance with Markable {
  
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
    
    def setMetaData( key: String, md : String ) : Unit = {
       hiveTable.setMetaData( key, md) 
    }

    def getMetaData( key: String ) : Option[String] = {
       hiveTable.getMetaData( key) 
    }
       /**
     *  Mark that the producer of this
     *   DataInstance fully completed .
     */
    def markCompleted : Unit = {
       hiveTable.setMetaData("isComplete" , "true")  
    }
    
    def markIncomplete : Unit = {
       hiveTable.setMetaData("isComplete" , "false")  
    } 

    /**
     *  Check that the Data instance has been Marked completed,
     *    according to the test of the markable.
     */
    def isMarkedCompleted : Boolean = {
      getMetaData("isComplete") match {
        case Some( check) => {
           check.toBoolean
        }
        case None => false
      }
    }

}