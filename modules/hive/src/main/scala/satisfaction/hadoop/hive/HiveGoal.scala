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
        
        val tblVariables = hiveOutput match {
          case tbl : HiveTable => 
              tbl.ms.getVariablesForTable(tbl.dbName, tbl.tblName)
          case partitionGrp : HiveTablePartitionGroup =>
            partitionGrp.variables
          	
        }
        val tblOutputs = collection.Set(hiveOutput.asInstanceOf[Evidence])
        
        println(" HIVE GOAL AUX JARS = " + hiveConf.getAuxJars)
        println(" HIVE GOAL AUX JARS PROP = " + hiveConf.get("hive.aux.jars.path"))
        
        val hiveFactory : SatisfierFactory = Goal.SatisfierFactory( {
            println(" FACTORY HIVE GOAL AUX JARS = " + hiveConf.getAuxJars)
            println(" FACTORY HIVE GOAL AUX JARS PROP = " + hiveConf.get("hive.aux.jars.path"))
           new HiveSatisfier(queryResource,hiveConf)
        })

        new Goal(name = name,
            satisfierFactory = hiveFactory,
            variables = tblVariables,
            depends,
            evidence = tblOutputs) 
    }

}