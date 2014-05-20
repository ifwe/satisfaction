package com.klout
package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.hadoop.hive.metastore.api.Database
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.conf.Configuration
import java.util.HashMap
import java.util.Map
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import fs._
import hdfs.HdfsImplicits._
import org.apache.hadoop.fs.{Path => ApachePath}
import org.apache.hadoop.fs.{FileStatus => ApacheFileStatus}
import org.apache.hadoop.fs.{FileSystem => ApacheFileSystem}


//import org.apache.hadoop.hive.metastore.api.Partition

/**
 *  Replicator replicates one MetaStore into another
 *   XXX TODO
 *     Set up Federation, by altering certain properties
 *      in the new MetaStore
 *      
 *  XXX Make a regular class, not object     
 *   parameterize certain features ( like dest url, number of days to replicate )    
 *    whether to Distcp data 
 *    
 *     
 *   
 */
object Replicator {
   val YYYYMMDD: DateTimeFormatter = DateTimeFormat.forPattern("YYYYMMdd")

   def getEarliestDate : DateTime = {
     val thirtyDaysAgo = DateTime.now.minusDays( 30)
     
     thirtyDaysAgo
   }
    /*
   *  
   */
    def replicateDatabase(fromMs: Hive, toMs: Hive, dbName: String) = {
        var toDb: Database = toMs.getDatabase(dbName)
        val fromDb = fromMs.getDatabase(dbName)
        if (toDb == null) {
            System.out.println(" Creating Database " + dbName)
            fromDb.setLocationUri( movedLocation( new Path(fromDb.getLocationUri), JOBS_DEV).toString )
            toMs.createDatabase(fromDb)
            toDb = toMs.getDatabase(dbName)
        } else {
            System.out.println(" Altering Database " + dbName)
            fromDb.setLocationUri( movedLocation( new Path(fromDb.getLocationUri()), JOBS_DEV).toString )
            toMs.alterDatabase(dbName, fromDb)
        }

        val fromTables = fromMs.getTablesByPattern(dbName, "*")
        fromTables.toList.map { tblName =>
        if ( tblName.compareTo("b") > 0 ) {
                try {
                    val fromTable = fromMs.getTable(dbName, tblName)
                    if (isHBase(fromTable)) {
                        System.out.println(" Skipping HBase table " + fromTable.getTableName())
                    } else
                        replicateTable(fromMs, toMs, fromTable)

                } catch {
                    case npe: NullPointerException =>
                        System.out.println(" Corrupt table " + tblName + " ; Skipping ... ")
                    case unexpected: Throwable =>
                        System.out.println(" Unexpected exception while loading  " + tblName + " ; Skipping ... ")
                }
            } else {
                println("Skipping already loaded table " + tblName)
            }
        }
    }

    def isHBase(tbl: Table): Boolean = {
        tbl.getInputFormatClass().getName().equals("org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat")
    }
    
    def movedLocation( oldLoc : ApachePath , dest : Path) : Path = {
      val oldLocURI = oldLoc.toUri
      val destURI = dest.toUri
      val newLoc = new java.net.URI( destURI.getScheme(),
          destURI.getUserInfo(),
          destURI.getHost(),
          destURI.getPort(),
          oldLocURI.getPath(),
          oldLocURI.getQuery(),
          oldLocURI.getFragment() )
    		  		
     
      new ApachePath(newLoc)
    }
    
    /// XXX set dest path as value on Replicator object
    val JOBS_DEV = new Path("hdfs://jobs-dev-hnn:8020")

    /**
     *  Replicate a table from one database to another data
     */
    def replicateTable(fromMs: Hive, toMs: Hive, fromTable: Table) = {
        System.out.println(" Replicating Table " + fromTable.getTableName())
        val oldTbl = toMs.getTable(fromTable.getDbName(), fromTable.getTableName(), false)
        if (oldTbl == null) {
           fromTable.setDataLocation( movedLocation( fromTable.getDataLocation , JOBS_DEV))
         
            val toTable = toMs.createTable(fromTable)
            ///oldTbl = toMs.getTable( fromTable.getDbName(), fromTable.getTableName())
            
        } else {
            fromTable.setDataLocation( movedLocation( fromTable.getDataLocation , JOBS_DEV))
            toMs.alterTable(fromTable.getTableName(), fromTable)
        }
        if (fromTable.isPartitioned()) {
            ///val partList = fromMs.getPartitions( fromTable)
            ///System.out.println( "  Adding " + partList.size + " partitions to " + fromTable.getTableName() )
            ///toMs.alterPartitions( fromTable.getTableName(), partList)

            val partNameList = fromMs.getPartitionNames(fromTable.getDbName(), fromTable.getTableName(), 15000)
            partNameList.map { partName =>
                System.out.println(" Transferring partition " + partName)
                try {
                    val partSpec = getPartitionSpecFromName(partName)
                    println(" PartSpec is " + partSpec)

                    val dt = YYYYMMDD.parseDateTime(partSpec.get("dt"))
                    if (dt.isAfter(getEarliestDate)) {
                        val oldPart = fromMs.getPartition(fromTable, partSpec, false)
                        
                        println(" Old part is  " + partSpec)
                        println(" Old part path  is  " + oldPart.getDataLocation)
                         oldPart.setLocation( movedLocation( oldPart.getDataLocation, JOBS_DEV).toString() )

                        val newPart = toMs.getPartition(fromTable, getPartitionSpecFromName(partName), true, oldPart.getDataLocation.toString(), true)
                        println(" New Part is " + newPart)
                        ///val newPart = toMs.createPartition(oldTbl, partSpec )
                        toMs.alterPartition(fromTable.getTableName(), oldPart)
                    } else {
                        println(" Skipping old partition " + partName);
                    }

                } catch {
                    case ill: IllegalArgumentException =>
                        println(" Something is wrong with partition " + partName)
                    case hve: HiveException =>
                        if (hve.getMessage().contains("new partition path should not be null or empty")) {
                            System.out.println("Skipping bogus partition " + partName)
                        } else {
                            throw hve
                        }
                }
            }
        }
    }

    def getPartitionSpecFromName(partName: String): java.util.Map[String, String] = {
        val retMap = new HashMap[String, String]()

        val partCols: Array[String] = partName.split('/')
        partCols.map { kvStr =>
            val kv = kvStr.split('=')
            retMap.put(kv(0).toString, kv(1).toString)
        }

        return retMap
    }
    
    
    def setDatabaseLocation( ms: MetaStore,  dbName : String, dbLoc : String) = {
      val hvDb  = ms.hive.getDatabase(dbName)
      hvDb.setLocationUri(dbLoc)
      hvDb.setLocationUriIsSet(true)
      
      ms.hive.alterDatabase(dbName, hvDb)
      
    }

    def main3(argv: Array[String]): Unit = {
        val stageHc = new HiveConf(new Configuration(), this.getClass())
        stageHc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-dev-hive1:9085")
        val toMs = new MetaStore(stageHc)
        println(" Destination MetaStore = " + toMs)
        

        ///val now = MetaStore.YYYYMMDD.parseDateTime("20130729")
        setDatabaseLocation(toMs, "bi_maxwell", "hdfs://jobs-dev-hnn/user/hive/warehouse/bi_maxwell.db")
      
    }
    def main(argv: Array[String]): Unit = {

        ///val toMs: MetaStore = MetaStore

        /// insights
        var hc = new HiveConf(new Configuration(), this.getClass())
        ///hc.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "jdbc:mysql://jobs-dev-sched1/hive_old_insights")
        ///hc.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "com.mysql.jdbc.Driver")
        ///hc.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "hive")
        ///hc.setVar(HiveConf.ConfVars.METASTOREPWD, "hiveklout")
        ///hc.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "jdbc:mysql://mysql-hive1/hive_meta_db")
        ///hc.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "com.mysql.jdbc.Driver")
        ///hc.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "hive")
        ///hc.setVar(HiveConf.ConfVars.METASTOREPWD, "hiveklout")

        val prodHc = new HiveConf(new Configuration(), this.getClass())
        ///prodHc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-dev-hive1:9085")
        prodHc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-aa-sched1:9083")
        val fromMs = new MetaStore(prodHc)
        println(" Production MetaStore = " + fromMs)
        
        val stageHc = new HiveConf(new Configuration(), this.getClass())
        stageHc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-dev-hive2:9085")
        val toMs = new MetaStore(stageHc)
        println(" Destination MetaStore = " + toMs)

        ///val now = MetaStore.YYYYMMDD.parseDateTime("20130729")
        ///fromMs.prunePartitionsByRetention("bi_insights", "agg_moment", now,  92)
        //////fromMs.prunePartitionsByRetention("bi_insights", "actor_action", now,  92)
        ////fromMs.prunePartitionsByRetention("bi_insights", "ksuid_mapping", now,  30)

        ////fromMs.cleanPartitionsForDb("bi_insights")

        ///replicateDatabase(fromMs.hive, toMs.hive, "bi_maxwell")
        ///replicateDatabase(fromMs.hive, toMs.hive, "bi_thunder")
        ///replicateDatabase(fromMs.hive, toMs.hive, "bing")
        ///val tbl = fromMs.getTableByName("bi_maxwell", "ksuid_mapping")
        //val tbl = fromMs.getTableByName("bi_maxwell", "ksuid_mapping")
        val tbl = fromMs.getTableByName("bi_maxwell", "wikipedia_extract")
        ////val tbl2 = fromMs.getTableByName("bi_maxwell", "actor_action")
        ///val tbl3 = fromMs.getTableByName("bi_maxwell", "fact_content")

        replicateTable( fromMs.hive, toMs.hive, tbl )
        //replicateTable( fromMs.hive, toMs.hive, tbl2 )
        ///replicateTable( fromMs.hive, toMs.hive, tbl3 )

    }

}