package hive.ms

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive._
import org.apache.hadoop.hive.conf._
import org.apache.hadoop.hive.metastore._
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.metadata._
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.shims.ShimLoader
import java.net.URI
import play.api.Logger
import java.util.HashMap
import org.joda.time._
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.hive.metastore.api.FieldSchema
import com.klout.satisfaction.Variable

/**
 *  Scala Wrapper around Hive MetaStore object
 *
 */

class MetaStore(hvConfig: HiveConf) {

    private val _hive = Hive.get(hvConfig)

    def hive(): Hive = { _hive }

    object MetaDataProps extends Enumeration {
        type Prop = Value
        val SPACE_USED = Value("spaceUsed")
        val SLA = Value("SLA")
    }

    def getDbs = {
        this.synchronized({
            _hive.getAllDatabases().toList
        })
    }

    def getTables(db: String) = {
        this.synchronized({
            _hive.getAllTables(db).toList
            /**
             * _hive.getAllTables( db).toList.filter( tbl =>
             * try{
             * _hive.getTable( db, tbl).getTableType() != TableType.VIRTUAL_VIEW
             * } catch {
             * case e:Exception => false
             * }
             * )
             *
             */
        })
    }

    def getPartitionNamesForTable(db: String, tblName: String): List[String] = {
        this.synchronized({
            _hive.getPartitionNames(db, tblName, 100).toList
        })
    }

    def getPartitionsForTable(tbl: Table): List[Partition] = {
        this.synchronized({ _hive.getPartitions(tbl).toList })
    }

    def getPartitionSetForTable(tbl: Table, partialVars: Map[String, String]) = {
        this.synchronized({ _hive.getPartitions(tbl, partialVars).toList })
    }

    def getPartitionSize(part: Partition): Long = {
        this.synchronized({
            val pMd = getPartitionMetaData(part)
            println(" MetaData is " + pMd)
            if (!pMd.contains(MetaDataProps.SPACE_USED.toString)) {

                val realPs: Long = Hdfs.getSpaceUsed(part.getPartitionPath())
                Logger.info(" Real Part size is " + realPs)
                println(" Real Part size is " + realPs)
                setPartitionMetaData(part, MetaDataProps.SPACE_USED.toString(), realPs.toString)
                return realPs
            } else {
                val ps = pMd.get(MetaDataProps.SPACE_USED.toString())
                println(" its in the META DATA " + ps + " LL " + ps.get)
                return ps.get.toLong
            }
        })
    }

    def getTableByName(db: String, tblName: String): Table = {
        this.synchronized({ _hive.getTable(db, tblName) })
    }

    /**
     *  For a table, clean up partitions which are either empty,
     *    or have no space in them
     */
    def cleanPartitions(db: String, tblName: String) = {
        this.synchronized({

            try {
                val tbl = _hive.getTable(db, tblName)
                if (!tbl.isView && tbl.isPartitioned()) {
                    _hive.getPartitions(tbl).toList.map { part =>
                        if (Hdfs.exists(part.getPartitionPath)) {
                            if (Hdfs.getSpaceUsed(part.getPartitionPath()) == 0) {
                                Logger.info("Dropping empty partition " + part.getValues + " for table " + tblName)
                                println("Dropping empty partition " + part.getValues + " for table " + tblName)
                                _hive.dropPartition(db, tblName, part.getValues(), true)
                                Hdfs.fs.delete(part.getPartitionPath())
                            } else {
                                Logger.info(" Keeping partition " + part.getValues + " for table " + tblName)
                                println(" Keeping partition " + part.getValues + " for table " + tblName)
                            }
                        } else {
                            Logger.info(" Dropping missing partition " + part.getValues + " for table " + tblName)
                            println(" Dropping missing partition " + part.getValues + " for table " + tblName)
                            _hive.dropPartition(db, tblName, part.getValues(), false)
                        }
                    }
                }
            } catch {
                case npe: NullPointerException =>
                    Logger.error("Unable to access table " + tblName + "; Error in Table.checkValidity")
            }
        })
    }

    val YYYYMMDD: DateTimeFormatter = DateTimeFormat.forPattern("YYYYMMdd")

    def prunePartitionsByRetention(db: String, tblName: String, now: DateTime, reten: Int) = {
        this.synchronized({
            val tbl = _hive.getTable(db, tblName)
            var dtIdx: Int = -1
            val partCols = tbl.getPartCols
            for (i <- 0 to partCols.size - 1) {
                if (partCols(i).getName().equals("dt")) {
                    println("dt idx = " + i)
                    dtIdx = i;
                }
            }
            val parts = _hive.getPartitions(tbl)
            println(" Pruning Partitions on table " + tbl.getCompleteName() + " for " + reten + " days from " + now)
            if (!tbl.isView && tbl.isPartitioned()) {
                parts.toList.map { part =>
                    println(" Checking partition  " + part.getName() + " with parameters " + part.getParameters())
                    val dtStr: String = part.getValues().get(dtIdx)
                    if (dtStr != null) {
                        val partDate = MetaStore.YYYYMMDD.parseDateTime(dtStr)
                        val numDays = Days.daysBetween(partDate, now).getDays()
                        println(" Number of days between " + partDate + " and  " + now + " = " + numDays)
                        if (numDays > reten) {
                            if (Hdfs.exists(part.getPartitionPath)) {
                                Logger.info("Deleting obsolete dated path " + part.getPartitionPath())
                                println("Deleting obsolete dated path " + part.getPartitionPath())
                                Hdfs.fs.delete(part.getPartitionPath())
                            }
                            Logger.info("Dropping obsolete partition " + part.getValues + " for table " + tblName)
                            println("Dropping obsolete partition " + part.getValues + " for table " + tblName)
                            _hive.dropPartition(db, tblName, part.getValues(), true)
                        } else {
                            Logger.info("Keeping recent partition " + part.getValues + " for table " + tblName)
                            println("Keeping recent partition " + part.getValues + " for table " + tblName)

                        }
                    }
                }
            }
        })
    }
    /**
     *  Clean the partitions for all partitioned tables within a database
     */
    def cleanPartitionsForDb(db: String) = {
        this.synchronized({
            val tblList = _hive.getTablesForDb(db, "*").toList
            tblList.map { tblName =>
                Logger.info(" Cleaning table " + db + "@" + tblName)
                println(" Cleaning table " + db + "@" + tblName)
                cleanPartitions(db, tblName)
            }
        })
    }

    /**
     *  Update the metadata to include disk space used on
     *    part files
     */
    def annotateSpaceUsed(db: String, tblName: String) = {

    }

    def getSpaceUsed(db: String, tblName: String): String = {
        getTableMetaData(db, tblName, MetaDataProps.SPACE_USED.toString())
    }

    def getSpaceUsedPartition(part: Partition): String = {
        getPartitionMetaData(part).get(MetaDataProps.SPACE_USED.toString()).toString
    }

    def getPartition(db: String, tblName: String, partMap: Map[String, String]): Partition = {
        this.synchronized({
            val tbl = _hive.getTable(db, tblName)
            _hive.getPartition(tbl, partMap, false)
        })
    }

    def getPartition(db: String, tblName: String, partSpec: List[String]): Partition = {
        this.synchronized({
            val tbl = _hive.getTable(db, tblName)
            val partMap = new HashMap[String, String]()
            val partCols = tbl.getPartCols()
            print("PArtCols = " + partCols)
            for (i <- 0 until partCols.size) {
                partMap.put(partCols.get(i).getName(), partSpec.get(i))
            }
            print(" PartMap = " + partMap)
            _hive.getPartition(tbl, partMap, false)
        })
    }

    def getPartitionByName(db: String, tblName: String, partName: String): Partition = {
        this.synchronized({
            val tbl = _hive.getTable(db, tblName)
            val partNameList = new java.util.ArrayList[String]()
            partNameList.add(partName)
            _hive.getPartitionsByNames(tbl, partNameList).get(0)
        })
    }

    /*
   *  delete  any partitions which are older than a given retention policy
   */
    ///def retainPartitions( db : String, tblName : String, dt : Date ) = {

    ///}

    /**
     *  Define a retention policy on a table to delete after a certain number of days
     *
     */
    def setRetentionPolicy(db: String, tblName: String, numDays: Int) = {
    }

    /**
     * Scan HDFS at the table location, and add partition matching certain
     *
     */
    def recoverPartitions(db: String, tblName: String) = {

    }

    /**
     *  Set a MetaData property on a Table
     *
     */
    def setTableMetaData(db: String, tblName: String, key: String, md: String) = {
        this.synchronized({
            val tbl = _hive.getTable(db, tblName)
            val map = tbl.getParameters()
            map.put(key, md)
            _hive.setCurrentDatabase(db)
            _hive.alterTable(tblName, tbl)
            println(" Set Table MetaData " + key + " :: " + md)
        })
    }

    /**
     *   Get the MetaData property on a table
     *
     */
    def getTableMetaData(db: String, tblName: String, key: String) = {
        this.synchronized({
            val tbl = _hive.getTable(db, tblName)
            val map = tbl.getParameters
            map.get(key)
        })
    }

    def getPartitionMetaData(part: Partition): Map[String, String] = {
        this.synchronized ({
            part.getParameters.toMap
        })
    }

    def setPartitionMetaData(part: Partition, key: String, md: String) = {
        this.synchronized({
            val map = part.getParameters
            map.put(key, md)
            val tblName: String = part.getTable().getTableName()
            _hive.setCurrentDatabase(part.getTable().getDbName())
            _hive.alterPartition(tblName, part)
        })
    }

    def getTableMetaData(db: String, tblName: String): Map[String, String] = {
        this.synchronized({
            val tbl = _hive.getTable(db, tblName)
            println("Table Metat Data is " + tbl.getParameters)
            tbl.getParameters.toMap
        })
    }

    /**
     *  Search the MetaStore to find all the views which depend upon a table
     *
     */
    def findDependentViews(db: String, tblName: String): List[Table] = {
        null
    }

    /**
     *  Given the name of a view, find all the tables that the view is ultimately
     *    dependent upon.
     */
    def getTableDependencies(db: String, viewName: String): List[Table] = {
        null
    }

    /**
     *  Define an SLA on a data asset,
     *   as to when the data should be created
     *
     */
    def defineSLA(db: String, tblName: String, slaType: String, sla: Partial, period: Period) = {
        val slaStr = slaType + "|" + sla.toString + "|" + period.toString
        setTableMetaData(db, tblName, MetaDataProps.SLA.toString, slaStr)
    }

    /**
     *  Set the goal associated with this table into metadata
     */
    def setGoalDefinition(db: String, tblName: String, goalDef: String) = {

    }

    def getVariablesForTable(db: String, tblName: String): Set[Variable[_]] = {
        val tbl = getTableByName(db, tblName)
        val partCols = tbl.getPartitionKeys().toList
        val vars = for (part <- partCols) yield {
            //// XXX Interpret partition type from column type
            //// Interpret "dt" as magical date type column
            val name = part.getName
            val typeName = part.getType
            val comment = part.getComment
            if (comment != null) {
                val param = new Variable(name, classOf[String], Some(comment))
                param
            } else {
                new Variable(name, classOf[String], None)

            }
        }
        vars.toSet
    }
}

/**
 *  Companion object
 */
object MetaStore extends MetaStore(Config.config) {

    ///val YYYYMMDD : DateTimeFormatter = DateTimeFormat.forPattern("YYYYMMdd")
    def main(argv: Array[String]): Unit = {

        val toMs: MetaStore = MetaStore

        /// insights
        ///fromMs.prunePartitionsByRetention("bi_insights", "agg_moment", now,  92)
        ///fromMs.prunePartitionsByRetention("bi_insights", "actor_action", now,  92)
        ///fromMs.prunePartitionsByRetention("bi_insights", "ksuid_mapping", now,  30)

        ///fromMs.cleanPartitionsForDb("bi_insights")

        //replicateTable( fromMs.hive, toMs.hive, tbl )
        val md = toMs.getTableMetaData("bi_maxwell", "agg_moment")
        md.foreach {
            case (k, v) =>
                println(" Key = " + k + " ; Value = " + v)
        }
        md.keys.map { k =>
            println(" Key = " + k)
        }

    }

}