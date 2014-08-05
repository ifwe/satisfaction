package satisfaction
package hadoop
package hive

import ms.MetaStore
import ms.HiveTable
import ms.HiveDataOutput
import ms.HiveTablePartitionGroup
import satisfaction._
import scala.io.Source
import org.apache.hadoop.hive.conf.HiveConf

/** 
 *   A Hive Goal executes a Hive Query
 *      to produce either a Hive Table,
 *      or a set of partitions in a HiveTable
 */
object HiveGoal {

    def apply(name: String,
              queryResource: String,
              hiveOutput: HiveDataOutput,
              depends: Set[(Witness => Witness, Goal)] = Set.empty )
        (implicit track : Track )
            : Goal = {

        implicit val hiveConf : HiveConf =  Config(track)
        val driver =  HiveDriver(hiveConf) 
        
        //// Hack to set the track XXX FIXME
        driver.asInstanceOf[HiveLocalDriver].track = track
        
        val hiveSatisfier = new HiveSatisfier(queryResource,
            driver)
        
        val tblVariables = hiveOutput match {
          case tbl : HiveTable => 
              tbl.ms.getVariablesForTable(tbl.dbName, tbl.tblName)
          case partitionGrp : HiveTablePartitionGroup =>
            partitionGrp.variables
          	
        }
        val tblOutputs = collection.Set(hiveOutput.asInstanceOf[Evidence])

        new Goal(name = name,
            satisfier = Some(hiveSatisfier),
            variables = tblVariables,
            depends,
            evidence = tblOutputs) {
        }
    }

}