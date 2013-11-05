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
 *      
 *      
 *  It scans a file, or a directory of HQL files, parses the HQL,
 *    and looks for update or create view in the 
 *      
 */
class DependencyTracker( val ms : MetaStore, val hiveDriver : HiveLocalDriver) {
   
  
    def init() {
      
    }
    
    
    
    def getDependencySetFromMetaStore( dbName : String ) : Map[DataOutput,Set[DataOutput]] = {
      
      null
    }
    
    def setDependencySetToMetaStore( depSet : Map[DataOutput,Set[DataOutput]]) = {
      
      null
    }
    
    
    

    def getDependenciesForView( dbName : String, viewName : String ) : Set[DataOutput] = {
     
       val selectViewCmd = "select * from " + dbName + "." + viewName 
       try {       
          hiveDriver.useDatabase( dbName)
          val qp = hiveDriver.getQueryPlan( selectViewCmd)
          if( qp != null) {
              println(" Query Plan for View " + viewName + " == " + qp.getQueryString())
              qp.getInputs.map( re => {
               println( " View " + viewName + " :: Read Entity is "  + re.getType + " Table " + re.getTable)
               (new HiveTable( dbName, re.getTable().getTableName())).asInstanceOf[DataOutput]
                   }).toSet
          }
          else  {
             println("QueryPlan is Null !!!")
             Set.empty
          } 
       
       
       } catch { 
         case unexpected : Throwable =>
            println(" Error " + unexpected + "  while trying to get Query plan on view  " + viewName )
            unexpected.printStackTrace
            Set.empty
       }
    }
    
    
    /**
     *  Go through all tables, and update the metastore 
     *   to reflect which tables created which view 
     */
    def updateDependencyMetaData() = {
      hiveDriver.sourceFile("oozie-setup.hql")
      val tableMap : collection.mutable.Map[HiveTable,List[String]] = new collection.mutable.HashMap[HiveTable,List[String]]()
      ///ms.getDbs.foreach( dbName => 
      Set("bi_maxwell").foreach( dbName => {
        ms.getViews(dbName).foreach( viewName => {
            val viewDeps = getDependenciesForView(dbName, viewName)
            ms.setTableMetaData(dbName, viewName, "satisfaction_view_tbl_inputs", viewDeps.mkString(","))
            viewDeps.foreach( dataOutput => {
               val hiveTable = dataOutput.asInstanceOf[HiveTable]
               tableMap.get( hiveTable) match {
                 case Some(viewList) => 
                 	 tableMap.put( hiveTable, viewList :+ viewName)
                 case None =>
                     tableMap.put( hiveTable, List[String]( viewName))
               }
            })
        })
      })
      
      println("XXX Adult Diapers -- Table Map Size is " + tableMap.size)
      tableMap.foreach{ case(hiveTable, viewList) => {
          println(" Setting MetaData on " + hiveTable.dbName + "::" + hiveTable.tblName + " == " + viewList.mkString(","))
          ms.setTableMetaData( hiveTable.dbName, hiveTable.tblName, "satisfaction_tbl_view_reader", viewList.mkString(","))   
        }
      }
      
    }
    
    /**
     *   Find tables which exist which weren't created by any view in the database,
     *     which have no tables which depend on them.
     *     
     *   These are tables which are candidates for deletion
     */
    def findCruftyTables( dbName : String ) : Set[DataOutput] = {
      //// Find tables which have no views reading into them
      /// find views where the partition doesn't exist anymore 
      //// Partitioned tables which have no partitions, and created a while ago
      
      null
    }
    
    /**
     *  Find the tables which were not created by any DataOutput 
     *    found in the metastore, for a database
     */
    def findSourceTables( dbName : String) : Set[DataOutput] = {
      
      null
    }
    
    /**
     *  Find the set of tables which have no DataOutputs
     *     which read from them
     */
    def findSinkTables( dbName : String ) : Set[DataOutput] = {
      
       null 
    }
    
    def collapseDependencySet( deps : Map[DataOutput,Set[DataOutput]] ) : Map[DataOutput,Set[DataOutput]] = {
      
      null
    }
    
    
    
}
   

object DependencyTracker extends DependencyTracker( MetaStore, new HiveLocalDriver) {
  
   def main(argv: Array[String]): Unit = {
	  updateDependencyMetaData
   }
   
}