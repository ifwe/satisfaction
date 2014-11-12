package satisfaction
package hadoop
package hive.ms

import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.hive.ql.metadata.{Table => ApacheTable}
import org.apache.hadoop.fs.{Path => ApachePath}
import collection.JavaConversions._
import fs._
import hdfs.Hdfs
import hdfs.HdfsPath
import hdfs.HdfsFactoryInit
import hdfs.HdfsImplicits._
import satisfaction.hadoop.hdfs.VariablePath
import org.joda.time.Period

trait HiveDataOutput extends DataOutput

/**
 *  Implies a table with Partitions
 */
case class HiveTable (
    dbName: String,
    tblName: String,
    isPartitioned: Boolean = true)
    (implicit val ms : MetaStore,
     implicit val hdfs : FileSystem) extends HiveDataOutput  with Logging {

    
    val checkMarkedComplete = true
    
    
    private lazy val _hiveTable : ApacheTable =   ms.getTableByName(dbName, tblName) 

    lazy val variables : List[Variable[_]] = {
      if(isPartitioned) {
    	 ms.getVariablesForTable(dbName, tblName)
      } else 
        List.empty
    }

    override def exists(w: Witness): Boolean = {
       if(variables.forall( w.contains( _ ))) {
         if(isPartitioned) {
           partitionExists( w)
         } else {
           /// XXX Unit test this ..
           _hiveTable != null
         }
       } else {
         warn(" Not all variables are saturated ; Use HivePartitionGroup instead of HiveTable")
         //// Try getting all the partitions for the table 
         val partitionList =  ms.getPartitionSetForTable(_hiveTable, w.raw)
         //// If there is at least one partition, say it exists; 
         ///// Otherwise use a  HivePartitionGroup , instead of HiveTable
         partitionList.size > 0
       }
    }
    
    
    def partitionExists(w: Witness): Boolean = {
      val partitionOpt = getPartition( w)
      info(s" GetPartition for $w is $partitionOpt")
      
      if(partitionOpt.isDefined ) {
        info(s" PARTITION EXISTS IS DEFINED" )
        if(  this.checkMarkedComplete) {
        	val partition = partitionOpt.get
        	info(s" IS MARKED COMPETE ${partition.isMarkedCompleted} ")
        	partition.isMarkedCompleted
        } else{
          true
        } 
      } else {
        false
      }
    }

    def getDataInstance(w: Witness): Option[DataInstance] = {
      if( isPartitioned ) {
        if(variables.forall( w.contains(_))) {
          val partition = getPartition(w)
          partition match {
            case Some(part) => Some(part)
            case None => None 
          }
        } else {
          /// Data instance is 
          val partialVars = w.raw.filterKeys( variables.map( _.name).contains(_))
          val parts = ms.getPartitionSetForTable(_hiveTable, partialVars).toSet
          val hivePartSet = new HivePartitionSet( parts.map( new HiveTablePartition(_))) 
          Some(hivePartSet)
        }
      } else {
         Some(new NonPartitionedTable( this))   
       }
    }
    
    
    def getPartition(witness: Witness): Option[HiveTablePartition] =  {
        try {
            
            val tblWitness = witness.filter( variables.toSet)
            debug( "variables for table is " + variables  +  " ; Witness variables = " + witness.variables)
            debug(s" TableWitness = $tblWitness == regular witness = $witness")
            val part = ms.getPartition(dbName, tblName, tblWitness.raw)

            if( part != null) {
              Some(HiveTablePartition(part))
            } else {
              None
            }
        } catch {
            case e: NoSuchElementException =>
              None
        }
        
    }
    
    def partitions : Seq[HiveTablePartition] = {
      ms.getPartitionsForTable(this._hiveTable).map( HiveTablePartition(_))
    }
    
    def addPartition(witness : Witness)( implicit track : Track) : HiveTablePartition = {
        val varWit = witness.assignments.filter( va => { variables.contains( va.variable ) } )

        val location = partitionPath.getPathForWitness( witness) 
        val part = ms.addPartition( dbName, tblName , Witness(varWit).raw , location )    
        
        partitionPath.getPathForWitness( witness) match {
          case Some(path) =>
            part.setLocation(path.toString)
            ms.alterPartition(dbName, tblName, part)
          case None =>
            warn(" Couldn't get partition path for some reason ")
        }
        HiveTablePartition(part)
    }

    def dropPartition(witness : Witness)( implicit track : Track) : Unit = {
       getPartition( witness ) match {
         case Some( part) => { part.drop }
         
         case None => { }
       }
    }
    
    
    
    
    def setMetaData( key : String, md : String ) : Unit = {
       ms.setTableMetaData(this.dbName, this.tblName, key, md) 
    }

    def getMetaData( key : String) : Option[String] = {
       ms.getTableMetaData(this.dbName, this.tblName, key) 
    }
    
    /**
     * Path for the location of the top leve directory for this table
     */
    def dataLocation : Path = {
       _hiveTable.getDataLocation()
    }
    
    
    /**
     *  Create a VariablePath describing the path for the partitions of this table
     */
    def partitionPath()(implicit track : Track) : VariablePath = {
       VariablePath( dataLocation, variables) 
    }
    
    def setRetentionPolicy( howLongToKeep : Period  ) : Unit = {
      setMetaData( MetaStore.RetentionMetaDataKey, howLongToKeep.toString() )
    }
    def getRetentionPolicy : Option[Period] = {
        getMetaData( MetaStore.RetentionMetaDataKey)  match {
          case Some(periodStr) =>  Some(Period.parse(periodStr))
          case None => None
        }
    }

}