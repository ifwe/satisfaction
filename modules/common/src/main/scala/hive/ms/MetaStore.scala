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
import com.klout.satisfaction.DataInstance
import com.klout.satisfaction.HiveTablePartition
import com.klout.satisfaction.DataOutput
import org.apache.hadoop.hive.metastore.api.MetaException
import scala.collection._

/**
 *  Scala Wrapper around Hive MetaStore object
 *
 */

class MetaStore(hvConfig: HiveConf) {

    private val _hive = Hive.get(hvConfig)
    private val _hdfs = new Hdfs( ("hdfs://jobs-dev-hnn:8020"))
    private var _dbList : List[String] = _initDbList
    private var _tableMap : collection.immutable.Map[String,List[String]] = _initTableMap 
    private var _viewMap : collection.immutable.Map[String,List[String]] = _initViewMap

    def hive(): Hive = { _hive }

    object MetaDataProps extends Enumeration {
        type Prop = Value
        val SPACE_USED = Value("spaceUsed")
        val SLA = Value("SLA")
    }

    private def _initDbList = {
        this.synchronized({
            _hive.getAllDatabases().toList
        })
    }
    
    def getDbs = {
      _dbList
    }

    def getTables(db: String) : List[String]= {
        _tableMap.get(db).get
    }
    
    def getViews(db: String) : List[String]= {
        _viewMap.get(db).get
    }
    
    
    /**
     *  Since tables cached, we may periodically need to reload them
     */
    def reloadMetaStore() = {
        _dbList = _initDbList
        _tableMap = _initTableMap 
        _viewMap = _initViewMap
        true
    }


    private def _initTableMap :  collection.immutable.Map[String,List[String]] = {
      this.synchronized({
       	 var buildMap : immutable.Map[String,List[String]]= Map.empty
        _dbList.foreach( db => {
        	buildMap = buildMap + ( db ->
        	_hive.getAllTables( db).toList.filter( tbl =>
              try{
                println(s" Getting table $db :: $tbl")
        	   _hive.getTable( db, tbl).getTableType() != TableType.VIRTUAL_VIEW
              } catch {
        	    case e:Throwable =>
                  println("Ignoring ..Unable to get table " + tbl + " Exception " + e)
        		  false
                })
        	 )
         })
         buildMap
      })
    }
    
    private def _initViewMap :  collection.immutable.Map[String,List[String]] = {
      this.synchronized({
       	 var buildMap : immutable.Map[String,List[String]]= Map.empty
         _dbList.foreach( db => {
        	buildMap = buildMap + ( db ->
        	_hive.getAllTables( db).toList.filter( tbl =>
              try{
                println(s" Getting view $db :: $tbl")
        	   _hive.getTable( db, tbl).getTableType() == TableType.VIRTUAL_VIEW
              } catch {
        	    case e: Throwable  =>
                  println(" Ignore ..Unable to get table " + tbl + " Exception " + e)
        		  false
                })
        	 )
         })
         buildMap
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

                val realPs: Long = _hdfs.getSpaceUsed(part.getPartitionPath())
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
                        if (_hdfs.exists(part.getPartitionPath)) {
                            if (_hdfs.getSpaceUsed(part.getPartitionPath()) == 0) {
                                Logger.info("Dropping empty partition " + part.getValues + " for table " + tblName)
                                println("Dropping empty partition " + part.getValues + " for table " + tblName)
                                _hive.dropPartition(db, tblName, part.getValues(), true)
                                _hdfs.fs.delete(part.getPartitionPath())
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
                case noClass: NoClassDefFoundError =>
                    Logger.error(" Ignoring HBase table, or table with undefined output format")
                case metaExc: MetaException =>
                    Logger.error(" Unexpected MetaException " + metaExc) 
                case runtime: RuntimeException =>
                    Logger.error(" Unexpected RuntimeException " + runtime) 
            }
        })
    }


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
                            if (_hdfs.exists(part.getPartitionPath)) {
                                Logger.info("Deleting obsolete dated path " + part.getPartitionPath())
                                println("Deleting obsolete dated path " + part.getPartitionPath())
                                _hdfs.fs.delete(part.getPartitionPath())
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
              if( tblName.compareTo( "ksuid_mapping") > 0) {
                Logger.info(" Cleaning table " + db + "@" + tblName)
                println(" Cleaning table " + db + "@" + tblName)
                cleanPartitions(db, tblName)
              }
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
    
    
    /**
     * XXX  Add to HiveTable and HivePartition DataInstances ..
     */
    def getRecentTime( ethr : Either[Table,Partition]) : DateTime = {
      ethr match {
        case Left( tbl ) =>
          val lat =  tbl.getLastAccessTime
          if( lat != 0)
             return new DateTime( lat*10000)
        
          val create = tbl.getTTable.getCreateTime
          if(create != 0) {
            return new DateTime( create*10000)
          }
        case Right(part) =>
          val lat = part.getLastAccessTime()
          if( lat != 0)
            return new DateTime( lat*10000)
          
          val create = part.getTPartition.getCreateTime
          if(create != 0) {
            return new DateTime( create*10000)
          }
          
      }
      
      null
    }
    
    
    /**
     *  Bucket the data according to activity, 
     *    so that we can replicate only necessary data,
     *  and that we can clean up unneeded tables and parttions   
     */
    def getTablesByActivity( db: String, periodDates : Seq[DateTime]) : Map[Interval,Seq[Either[Table,Partition]]] = { 
      
      val map =  collection.mutable.HashMap[Interval,Seq[Either[Table,Partition]]]()
      val periods = MetaStore.getIntervalsForDates( periodDates)
      periods.foreach( per => {
        println( " Putting buffers in for period " + per.toString)
    	  map.put( per, new collection.mutable.ArrayBuffer[Either[Table,Partition]])
      })
      
      
      val tables = _hive.getAllTables( db)
      println(" Number of tables is " + tables.size)
      tables.foreach( tblName => {
          println(" Processing table "+ tblName)
          val tbl = _hive.getTable( db, tblName)
          if(! tbl.isView)
            if( tbl.isPartitioned() ) {
              val parts = _hive.getPartitions(tbl)
              parts.foreach( part => {
               val partDt = getRecentTime( Right(part)) 
               println(" part last access time is " + partDt)
               periods.foreach( per => {
                if( per.contains(partDt) ) {
                   val buffer = map.get(per).get
                   buffer  :+ tbl
                 }
               })
              })
            } else {
            
              /// Change to be create time if last access is 0
              val tblDt =  getRecentTime(Left(tbl))
              println(" Table last access time is " + tblDt)
              periods.foreach( per => {
                 if( per.contains(tblDt) ) {
                   val buffer = map.get(per).get
                   buffer :+ tbl
                }
              })
            }
       } )
      
      map.toMap
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
     * Scan _hdfs at the table location, and add partition matching certain
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
     *  Set metadata for specific Hive Partition
     */
    def setInstanceMetaData(di: DataInstance, key: String, md: String): Unit = {
        di match {
            case part: HiveTablePartition =>
                setPartitionMetaData(part.part, key, md)
            /// XXX find some way to store other metadata
        }
    }

    /**
     *  Set MetaData for abstract outputs
     */
    def setOutputMetaData(dataOut: DataOutput): Unit = {

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
     *    XXX As derived from metadata ???0
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
     *   
     */
    def setGoalDefinition(db: String, tblName: String, goalDef: String) = {

    }

    def getVariablesForTable(db: String, tblName: String): collection.immutable.Set[Variable[_]] = {
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
    def apply( msURI : java.net.URI ) : MetaStore = {
      val conf = Config.initHiveConf
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, msURI.toASCIIString())
      
      new MetaStore(conf)
    }
  
    /**
     *  From a set of dates 
     *  XXXX Move to time utility class
     */
    def getIntervalsForDates( dates : Seq[DateTime]) : Seq[Interval] = {
      val backward = dates.sortWith( (dt1, dt2) => dt1.isAfter(dt2) )
       backward.foldLeft[Seq[Interval]]( Seq[Interval]() )( (seqP, dt) => {
         var lastDt :DateTime = null 
         if( lastDt != null) {
           println(" Current DT = " + dt + " ; Last DT =- " + lastDt)
           val newInterval = new Interval( dt, lastDt) 
           lastDt = dt
           seqP :+ newInterval
          } else {
           lastDt = dt
           seqP 
          }
      } )
    }
      


    val YYYYMMDD : DateTimeFormatter = DateTimeFormat.forPattern("YYYYMMdd")
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