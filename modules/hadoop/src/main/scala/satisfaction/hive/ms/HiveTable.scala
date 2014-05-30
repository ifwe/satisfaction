package com.klout
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

/**
 *  Implies a table with Partitions
 */
case class HiveTable (
    dbName: String,
    tblName: String,
    isPartitioned: Boolean = true)
    (implicit val ms : MetaStore,
     implicit val hdfs : FileSystem) extends DataOutput  {

    
    val checkSuccessFile = true
    
    
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
         //// XXX What to check exactly ???
         //// 
         println(" Not all variables are saturated ; Use HivePartitionGroup instead of HiveTable")

         false 
       }
    }
    
    
    def partitionExists(w: Witness): Boolean = {
      val partitionOpt = getPartition( w)
      
      if(partitionOpt.isDefined ) {
        if( checkSuccessFile) {
        	val partition = partitionOpt.get
        	println(s" PARTITION = $partition")
        	if( hdfs.exists( partition.path)) {
        	  /// Check metadata to see if table has a min partition size 
        	   true 
        	} else {
        	  false
        	}
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
            println( "variables for table is " + variables  +  " ; Witness variables = " + witness.variables)
            println(s" TableWitness = $tblWitness == regular witness = $witness")
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
    
    def addPartition(witness : Witness)( implicit track : Track) : HiveTablePartition = {
        val varWit = witness.assignments.filter( va => { variables.contains( va.variable ) } )
        val part = ms.addPartition( dbName, tblName , Witness(varWit).raw  )    
        
        partitionPath.getPathForWitness( witness) match {
          case Some(path) =>
            part.setLocation(path.toString)
            ms.alterPartition(dbName, tblName, part)
          case None =>
            println("WARN Couldn't get path for some reason ")
        }
        HiveTablePartition(part)
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

}