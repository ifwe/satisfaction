package com.klout
package satisfaction
package hadoop
package hive

import ms.MetaStore
import ms.HiveTable
import scala.io.Source
import org.apache.hadoop.hive.conf.HiveConf

/**
 */
object HiveGoal {

    def apply(name: String,
              queryResource: String,
              table: HiveTable,
              depends: Set[(Witness => Witness, Goal)] = Set.empty )
        (implicit track : Track )
            : Goal = {

        implicit val hiveConf : HiveConf =  Config(track)
        val driver =  HiveDriver(hiveConf) 
        val hiveSatisfier = new HiveSatisfier(queryResource,
            driver)
        
        val tblVariables = table.ms.getVariablesForTable(table.dbName, table.tblName)
        val tblOutputs = collection.Set(table.asInstanceOf[Evidence])

        new Goal(name = name,
            satisfier = Some(hiveSatisfier),
            variables = tblVariables,
            depends,
            evidence = tblOutputs) {
          
            
        }
    }

}