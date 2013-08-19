package hive.ms

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.Table
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

/**
 *  Replicator replicates one MetaStore into another
 *   XXX TODO
 *     Set up Federation, by altering certain properties
 *      in the new MetaStore
 */
object Replicator {

    /*
   *  
   */
    def replicateDatabase(fromMs: Hive, toMs: Hive, dbName: String) = {
        var toDb: Database = toMs.getDatabase(dbName)
        val fromDb = fromMs.getDatabase(dbName)
        if (toDb == null) {
            System.out.println(" Creating Database " + dbName)
            toMs.createDatabase(fromDb)
            toDb = toMs.getDatabase(dbName)
        } else {
            System.out.println(" Altering Database " + dbName)
            toMs.alterDatabase(dbName, fromDb)
        }

        val fromTables = fromMs.getAllTables(dbName)
        fromTables.toList.map { tblName =>
            if (tblName.compareTo("hb") > 0) {
                try {
                    val fromTable = fromMs.getTable(dbName, tblName)
                    if (isHBase(fromTable)) {
                        System.out.println(" Skipping HBase table " + fromTable.getTableName())
                    } else
                        replicateTable(fromMs, toMs, fromTable)

                } catch {
                    case npe: NullPointerException =>
                        System.out.println(" Corrupt table " + tblName + " ; Skipping ... ")
                }
            } else {
                println("Skipping already loaded table " + tblName)
            }
        }
    }

    def isHBase(tbl: Table): Boolean = {
        tbl.getInputFormatClass().getName().equals("org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat")
    }

    /**
     *  Replicate a table from one database to another data
     */
    def replicateTable(fromMs: Hive, toMs: Hive, fromTable: Table) = {
        System.out.println(" Replicating Table " + fromTable.getTableName())
        val oldTbl = toMs.getTable(fromTable.getDbName(), fromTable.getTableName(), false)
        if (oldTbl == null) {
            toMs.createTable(fromTable)
            ///oldTbl = toMs.getTable( fromTable.getDbName(), fromTable.getTableName())
        } else {
            toMs.setCurrentDatabase(fromTable.getDbName()) /// sic ..
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

                    val YYYYMMDD: DateTimeFormatter = DateTimeFormat.forPattern("YYYYMMdd")
                    val dt = YYYYMMDD.parseDateTime(partSpec.get("dt"))
                    val d20130801 = YYYYMMDD.parseDateTime("20130810")
                    if (dt.isAfter(d20130801)) {
                        val oldPart = fromMs.getPartition(fromTable, partSpec, false)
                        println(" Old part is  " + partSpec)
                        println(" Old part path  is  " + oldPart.getPartitionPath())

                        val newPart = toMs.getPartition(fromTable, getPartitionSpecFromName(partName), true, oldPart.getPartitionPath().toString(), true)
                        println(" New Part is " + newPart)
                        ///val newPart = toMs.createPartition(oldTbl, partSpec )
                        toMs.setCurrentDatabase(fromTable.getDbName())
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

    def main(argv: Array[String]): Unit = {

        ///val toMs: MetaStore = MetaStore

        /// insights
        var hc = new HiveConf(new Configuration(), this.getClass())
        ///hc.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "jdbc:mysql://jobs-dev-sched1/hive_old_insights")
        ///hc.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "com.mysql.jdbc.Driver")
        ///hc.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "hive")
        ///hc.setVar(HiveConf.ConfVars.METASTOREPWD, "hiveklout")
        hc.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "jdbc:mysql://mysql-hive1/hive_meta_db")
        hc.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "com.mysql.jdbc.Driver")
        hc.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "hive")
        hc.setVar(HiveConf.ConfVars.METASTOREPWD, "hiveklout")
        val fromMs = new MetaStore(hc)

        val prodHc = new HiveConf(new Configuration(), this.getClass())
        prodHc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-aa-sched1:9083")
        val toMs = new MetaStore(prodHc)

        ///val now = MetaStore.YYYYMMDD.parseDateTime("20130729")
        ///fromMs.prunePartitionsByRetention("bi_insights", "agg_moment", now,  92)
        //////fromMs.prunePartitionsByRetention("bi_insights", "actor_action", now,  92)
        ////fromMs.prunePartitionsByRetention("bi_insights", "ksuid_mapping", now,  30)

        ////fromMs.cleanPartitionsForDb("bi_insights")

        replicateDatabase(fromMs.hive, toMs.hive, "bi_insights")
        ///val tbl = fromMs.getTableByName("bi_insights", "users_relationships")

        //replicateTable( fromMs.hive, toMs.hive, tbl )

    }

}