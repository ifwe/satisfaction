package satisfaction
package hadoop
package hive

import ms.MetaStore
import ms.HiveTable
import ms.HiveDataOutput
import ms.HiveTablePartitionGroup
import satisfaction._
import scala.io.Source
import _root_.org.apache.hadoop.hive.conf.HiveConf

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
        (implicit track : Track, hiveConf : HiveConf)
            : Goal = {

        
        val tblVariables = hiveOutput match {
          case tbl : HiveTable => 
              tbl.ms.getVariablesForTable(tbl.dbName, tbl.tblName)
          case partitionGrp : HiveTablePartitionGroup =>
            partitionGrp.variables
          	
        }
        val tblOutputs = collection.Set(hiveOutput.asInstanceOf[Evidence])
        
        
        val hiveFactory : SatisfierFactory = Goal.SatisfierFactoryFromFunc( () => {
           println(s" HIVE GOAL CREATING NEW HIVE SATISFIER ")
           val hs = new HiveSatisfier(queryResource,hiveConf)
           println(s" HIVE GOAL CREATING NEW HIVE SATISFIER $hs")
           hs
        })
        println(s" HIVE GOAL SATISFIER FACTIRO = $hiveFactory")

        new Goal(name = name,
            satisfierFactory = hiveFactory,
            variables = tblVariables,
            depends,
            evidence = tblOutputs) 
    }

}