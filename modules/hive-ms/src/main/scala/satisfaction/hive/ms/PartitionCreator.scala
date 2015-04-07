package satisfaction
package hadoop
package hive
package ms

import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.metadata.Hive
import satisfaction.fs.Path
import satisfaction.fs.FileSystem
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc
import org.joda.time.DateTime
import scala.collection.JavaConversions._
import hdfs.HdfsImplicits._
import satisfaction.fs.FileSystem

/**
 *   Scan a FileSystem, and make sure that partitions for a table
 *     get created if they don't exist
 *      
 */
class PartitionCreator( table : Table, ms : Hive )(implicit hdfs : FileSystem) extends Logging  {
  var lastTime : DateTime = DateTime.now.minusDays(1);

  def scanForNewPartitions() = {
    val addPartitions = new AddPartitionDesc( table.getDbName(), table.getTableName(), true) 
    val tablePath : Path = table.getDataLocation

    scanForNewDirs( hdfs, tablePath, addPartitions)
    
    ms.createPartitions(addPartitions)
    lastTime = DateTime.now
  }

  /**
   *  Derive the partition to create from a given Path
   *  
   *  Return the partition variables as a map, and the 
   *   relative location of the partition.
   */
  def getPartitionFromPath( path : Path ) : (Map[String,String],String) = {
    val partitionVars = table.getPartitionKeys()
    val pathDirs  = path.split
    val partitionDirs = pathDirs.drop( pathDirs.length - partitionVars.size )
   
    var idx = 0
    val partMap = partitionVars.map( partVar => { val partVal = partitionDirs(idx); idx += 1; (partVar.getName -> partVal ) } ).toMap
    val partPath = partitionDirs.mkString("/")
    
     (partMap, partPath)
  }

  def scanForNewDirs(  fs : FileSystem , rootPath : Path, addPartitions : AddPartitionDesc ) = {
     val pathVisitorFunc : Path=>Unit = ( path => {
        val (partMap,partPath) = getPartitionFromPath( path)
         info( s" Adding to Table ${table.getTableName()} Partition $partPath ")
         addPartitions.addPartition( partMap, partPath)
     } )
     PartitionCreator.scanForNewDirs( fs, rootPath, lastTime, pathVisitorFunc)
  }
  
}
object PartitionCreator {
    /**
     *  Scan a FileSystem to see if new files have been created, and then call a callback function
     *    for 
     */
    def scanForNewDirs( fs : FileSystem, rootPath : Path , lastTime : DateTime,  pathVisitor : Path => Unit ) = {
      fs.listFilesRecursively( rootPath).foreach( fstat => {
          if( fstat.isFile) {
             if( fstat.created.getMillis() > lastTime.getMillis() ) {
               val parent = fs.getStatus(fstat.path.parent)
               if( parent.created.getMillis() > lastTime.getMillis()) {
                 pathVisitor( parent.path)
               }
             }
          }
      } )
    }
  
}
