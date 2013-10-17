package hive.ms

import com.klout.satisfaction.DataOutput
import collection.JavaConversions._
import com.klout.satisfaction.HiveTable
import com.klout.satisfaction.DataOutput

/**
 *  Dependency Track manages the dependencies
 *     between views and tables in the Hive metastore
 *     and is useful in automatically handling 
 *      dependent tasks 
 */
class DependencyTracker {
   val ms : MetaStore = MetaStore
   
   /// Access to direct hive driver
   //val hiveDriver : org.apache.hadoop.hive.ql.Driver
   val hiveDriver : HiveLocalDriver = null
   

    def getDependenciesForView( dbName : String, viewName : String ) : Set[DataOutput] = {
     
       val selectViewCmd = "select * from " + viewName + " ; "
       val qp = hiveDriver.getQueryPlan( selectViewCmd)
       
       ///qp.getInputs.map( re => {
         ///println( " Read Entity is "  + re.getType + " Table " + re.getTable)
          ///(new HiveTable( dbName, re.getTable().getTableName())).asInstanceOf[DataOutput]
       ///})
       
       
       null
    }
   
    def findCruftyTables( dbName : String ) = {
      //// Find tables which have no views reading into them
      /// find views where the partition doesn't exist anymore 
      //// Partitioned tables which have no partitions, and created a while ago
      
    }
     
  
}