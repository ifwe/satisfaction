package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive._
import org.apache.hadoop.hive.conf._
import org.apache.hadoop.hive.metastore._
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.metadata._
import scala.collection.JavaConversions._
import scala.collection.JavaConversions
import org.apache.hadoop.hive.shims.ShimLoader
import java.net.URI
import java.util.HashMap
import org.joda.time._
import fs._
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.MetaException
import scala.collection._
import hdfs.Hdfs
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc


/**
 *  Scala Wrapper around Hive MetaStore object

 *
 */


///case class MetaStore(val hvConfig: HiveConf) extends Logging {
case class MetaStore(implicit val config: HiveConf)  extends Logging with java.io.Closeable {
   import hdfs.HdfsImplicits
   import hdfs.HdfsImplicits._

    private lazy val _hdfs = Hdfs.fromConfig(config)
    
    
    val PRELOAD = false
    private var _dbList : List[String] = if( PRELOAD)  { _initDbList  } else { null }
    private var _tableMap : collection.immutable.Map[String,List[String]] = if( PRELOAD) { _initTableMap  } else { null }
    private var _viewMap : collection.immutable.Map[String,List[String]] = if( PRELOAD ) { _initViewMap } else { null }

    private var _lastAccess : Long = 0
    def hive(): Hive = {
      /// Refresh every 5 min
      val refresh =  if( System.currentTimeMillis() - _lastAccess >  1000*60*5) {
         _lastAccess = System.currentTimeMillis();
         true
      } else {
        false
      }
      Hive.get( config, refresh)
    }
    
    def close() = {
       Hive.closeCurrent
    }

    object MetaDataProps extends Enumeration {
        type Prop = Value
        val SPACE_USED = Value("spaceUsed")
        val SLA = Value("SLA")
    }

    private def _initDbList = {
        this.synchronized({
          try {
            info(" Metastore URI  =  " + config.getVar( HiveConf.ConfVars.METASTOREURIS))
            info(" Init DB LIST !! hive =  " + hive)
            val list = hive.getAllDatabases().toList
            list.foreach( info(_))
            list
          } catch {
            case exc: Exception => {
              error("Problem initializing DB List" , exc)
              throw exc
            }
          }
        })
    }
    
    def getDbs = {
      if(_dbList == null ) _dbList = _initDbList
      _dbList
    }

    def getTables(db: String) : List[String]= {
        if(_tableMap == null ) _tableMap = _initTableMap
        _tableMap.get(db).get
    }
    
    def getViews(db: String) : List[String]= {
        if(_viewMap == null ) _viewMap = _initViewMap
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
           getDbs.foreach( db => {
        	buildMap = buildMap + ( db ->
        	hive.getAllTables( db).toList.filter( tbl =>
              try{
                debug(s" Getting table $db :: $tbl")
        	    hive.getTable( db, tbl).getTableType() != TableType.VIRTUAL_VIEW
              } catch {
        	    case e:Throwable =>
                  warn("Ignoring ..Unable to get table " + tbl + " Exception " + e)
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
          getDbs.foreach( db => {
        	buildMap = buildMap + ( db ->
        	hive.getAllTables( db).toList.filter( tbl =>
              try{
                debug(s" Getting view $db :: $tbl")
        	    hive.getTable( db, tbl).getTableType() == TableType.VIRTUAL_VIEW
              } catch {
        	    case e: Throwable  =>
                  warn(" Ignore ..Unable to get table " + tbl + " Exception " + e)
        		  false
                })
        	 )
         })
         buildMap
      })
    }
    

    def getPartitionNamesForTable(db: String, tblName: String): List[String] = {
        this.synchronized({
            hive.getPartitionNames(db, tblName, 100).toList
        })
    }

    def getPartitionsForTable(tbl: Table): Seq[Partition] = {
        this.synchronized({ hive.getPartitions(tbl).toList })
    }

    def getPartitionSetForTable(tbl: Table, partialVars: Map[String, String]) : Seq[Partition] = {
        this.synchronized({ hive.getPartitions(tbl, partialVars).toList })
    }

    def getPartitionSize(part: Partition): Long = {
        this.synchronized({
            val pMd = getPartitionMetaData(part)
            debug(" MetaData is " + pMd)
            if (!pMd.contains(MetaDataProps.SPACE_USED.toString)) {

                val realPs: Long = _hdfs.getStatus(part.getDataLocation).size
                debug(" Real Part size is " + realPs)
                setPartitionMetaData(part, MetaDataProps.SPACE_USED.toString(), realPs.toString)
                return realPs
            } else {
                val ps = pMd.get(MetaDataProps.SPACE_USED.toString())
                debug(" its in the META DATA " + ps + " LL " + ps.get)
                return ps.get.toLong
            }
        })
    }

    def getTableByName(db: String, tblName: String): Table = {
        this.synchronized({ hive.getTable(db, tblName) })
    }
    
    def tableExists( db: String, tblName: String) : Boolean = {
       this.synchronized( hive.getTable(db,tblName, false) != null)
    }
    
    def createTable( db: String, tbl : Table ) = {
      this.synchronized {
        tbl.setDbName(db)
        hive.createTable( tbl)
      }
      
    }

    /**
     *  For a table, clean up partitions which are either empty,
     *    or have no space in them
     */
    def cleanPartitions(db: String, tblName: String) = {
        this.synchronized({
            try {
                val tbl = hive.getTable(db, tblName)
                if (!tbl.isView && tbl.isPartitioned()) {
                    hive.getPartitions(tbl).toList.map { part =>
                        if (_hdfs.exists(part.getDataLocation)) {
                            if (_hdfs.getSpaceUsed(part.getDataLocation) == 0) {
                               debug("Dropping empty partition " + part.getValues + " for table " + tblName)
                                hive.dropPartition(db, tblName, part.getValues(), true)
                                _hdfs.fs.delete(part.getDataLocation)
                            } else {
                               debug(" Keeping partition " + part.getValues + " for table " + tblName)
                            }
                        } else {
                            debug(" Dropping missing partition " + part.getValues + " for table " + tblName)
                            hive.dropPartition(db, tblName, part.getValues(), false)
                        }
                    }
                }
            } catch {
                case npe: NullPointerException =>
                    error("Unable to access table " + tblName + "; Error in Table.checkValidity")
                case noClass: NoClassDefFoundError =>
                    error(" Ignoring HBase table, or table with undefined output format")
                case metaExc: MetaException =>
                    error(" Unexpected MetaException " + metaExc) 
                case runtime: RuntimeException =>
                    error(" Unexpected RuntimeException " + runtime) 
            }
        })
    }


    def prunePartitionsByRetention(db: String, tblName: String, now: DateTime, reten: Int) = {
        this.synchronized({
            val tbl = hive.getTable(db, tblName)
            var dtIdx: Int = -1
            val partCols = tbl.getPartCols
            for (i <- 0 to partCols.size - 1) {
                if (partCols(i).getName().equals("dt")) {
                    dtIdx = i;
                }
            }
            val parts = hive.getPartitions(tbl)
            info(" Pruning Partitions on table " + tbl.getCompleteName() + " for " + reten + " days from " + now)
            if (!tbl.isView && tbl.isPartitioned()) {
                parts.toList.map { part =>
                    debug(" Checking partition  " + part.getName() + " with parameters " + part.getParameters())
                    val dtStr: String = part.getValues().get(dtIdx)
                    if (dtStr != null) {
                        val partDate = MetaStore.YYYYMMDD.parseDateTime(dtStr)
                        val numDays = Days.daysBetween(partDate, now).getDays()
                        debug(" Number of days between " + partDate + " and  " + now + " = " + numDays)
                        if (numDays > reten) {
                            if (_hdfs.exists(part.getDataLocation)) {
                               info("Deleting obsolete dated path " + part.getDataLocation)
                                _hdfs.fs.delete(part.getDataLocation)
                            }
                            info("Dropping obsolete partition " + part.getValues + " for table " + tblName)
                            hive.dropPartition(db, tblName, part.getValues(), true)
                        } else {
                            info("Keeping recent partition " + part.getValues + " for table " + tblName)
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
            val tblList = hive.getTablesForDb(db, "*").toList
            tblList.map { tblName =>
              if( tblName.compareTo( "ksuid_mapping") > 0) {
                info(" Cleaning table " + db + "@" + tblName)
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
      /// XXX 
        getTableMetaData(db, tblName, MetaDataProps.SPACE_USED.toString()).get
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
    	  map.put( per, new collection.mutable.ArrayBuffer[Either[Table,Partition]])
      })
      
      
      val tables = hive.getAllTables( db)
      tables.foreach( tblName => {
          val tbl = hive.getTable( db, tblName)
          if(! tbl.isView)
            if( tbl.isPartitioned() ) {
              val parts = hive.getPartitions(tbl)
              parts.foreach( part => {
               val partDt = getRecentTime( Right(part)) 
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
            val tbl = hive.getTable(db, tblName)
            hive.getPartition(tbl, partMap, false)
        })
    }
   
    
    def addPartition(db: String, tblName: String, partMap: Map[String, String], pathOpt : Option[Path] = None): Partition = {
        this.synchronized({
          try { 
            val addPartitionDesc = new AddPartitionDesc(db,tblName, true)
            val path = pathOpt match {
              case Some(p) => p.toString
              case None => null
            }
            addPartitionDesc.addPartition( partMap, path )
            val newParts = hive.createPartitions( addPartitionDesc)
            if( newParts.size >0) {
              newParts.get(0)
            } else {
              //// Already exists ...
              val tbl = hive.getTable( db,tblName)
              hive.getPartition(tbl, partMap, false)
            }
          } catch {
            case alreadyExists : AlreadyExistsException => {
              /// Handle race condition, if some other job tried to create partition
              info(s" Race condition while trying to create partition on table $db $tblName ; Already Exists !!!")
              getPartition( db, tblName, partMap )
            } 
            case unexpected : Throwable => {
              throw unexpected
            }
          }
        })
    }
    
    def alterPartition( db: String, tblName: String , part: Partition )  = {
      this.synchronized {
        hive.alterPartition(db,tblName, part)
      }
    }
    
    def dropPartition( db: String, tblName: String , part: Partition , deleteData : Boolean = true)  = {
      this.synchronized {
        hive.dropPartition(db, tblName, part.getValues , deleteData)
      }
    }

    def getPartition(db: String, tblName: String, partSpec: List[String]): Partition = {
        this.synchronized({
            val tbl = hive.getTable(db, tblName)
            val partMap = new HashMap[String, String]()
            val partCols = tbl.getPartCols()
            for (i <- 0 until partCols.size) {
                partMap.put(partCols.get(i).getName(), partSpec.get(i))
            }
            hive.getPartition(tbl, partMap, false)
        })
    }

    def getPartitionByName(db: String, tblName: String, partName: String): Partition = {
        this.synchronized({
            val tbl = hive.getTable(db, tblName)
            val partNameList = new java.util.ArrayList[String]()
            partNameList.add(partName)
            hive.getPartitionsByNames(tbl, partNameList).get(0)
        })
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
            val tbl = hive.getTable(db, tblName)
            val map = tbl.getParameters()
            map.put(key, md)
            hive.alterTable(tblName, tbl)
        })
    }

    /**
     *   Get the MetaData property on a table
     *
     */
    def getTableMetaData(db: String, tblName: String, key: String) : Option[String] = {
        this.synchronized({
            val tbl = hive.getTable(db, tblName)
            val map = tbl.getParameters
            if ( map.containsKey(key)  ) {
              Some(map.get(key))
            } else {
              None
            }
        })
    }

    def getPartitionMetaData(part: Partition): Map[String, String] = {
        this.synchronized ({
            part.getParameters.toMap
        })
    }

    def getPartitionMetaData(part: Partition, key : String): Option[String] = {
        this.synchronized ({
            part.getParameters.toMap.get( key)
        })
    }

    def setPartitionMetaData(part: Partition, key: String, md: String) : Unit = {
        this.synchronized({
            val map = part.getParameters
            map.put(key, md)
            val tblName: String = part.getTable().getTableName()
            
                  hive.alterPartition(part.getTable().getDbName(), tblName, part)
        })
    }

    def getTableMetaData(db: String, tblName: String): Map[String, String] = {
        this.synchronized({
            val tbl = hive.getTable(db, tblName)
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

    def getVariablesForTable(db: String, tblName: String): List[Variable[_]] = {
        val tbl = getTableByName(db, tblName)
        tbl.getPartitionKeys.map( part => {
            val name = part.getName
            name match {
              case "dt" => TemporalVariable.Dt
              case "date" => TemporalVariable.Date
              case "hour" => TemporalVariable.Hour
              case "minute" => TemporalVariable.Minute
              case _ => {
                val typeName = part.getType
                val comment = part.getComment
                if (comment != null) {
                    new Variable(name, classOf[String], Some(comment))
                } else {
                    new Variable(name, classOf[String], None)
                }
              }
            }
          }
       ).toList
    }
}

/**
 *  Companion object
 */
object MetaStore  {
  
    def apply( msURI : java.net.URI )(implicit config :  HiveConf = new HiveConf(Config.config, classOf[MetaStore])) : MetaStore = {
      config.setVar(HiveConf.ConfVars.METASTOREURIS, msURI.toASCIIString())
      
      new MetaStore()(config)
    }
    
    	//// XXX JDB --- Put in hard-wired defaults here ...
    lazy val default = new MetaStore()(new HiveConf(Config.config, classOf[MetaStore] ))
  
    /**
     *  From a set of dates 
     *  XXXX Move to time utility class
     */
    def getIntervalsForDates( dates : Seq[DateTime]) : Seq[Interval] = {
      val backward = dates.sortWith( (dt1, dt2) => dt1.isAfter(dt2) )
       backward.foldLeft[Seq[Interval]]( Seq[Interval]() )( (seqP, dt) => {
         var lastDt :DateTime = null 
         if( lastDt != null) {
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

   
    val RetentionMetaDataKey = "satisfaction_retention"
    val LastModifiedMetaDataKey = "satisfaction_last_modified"
    val IsCompleteMetaDataKey = "satisfaction_is_complete"
    
}