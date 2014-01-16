package com.klout
package satisfaction
package hadoop
package hive

import ms.MetaStore
import ms.HiveTable
import scala.io.Source

/**
 */
object HiveGoal {


    def apply(name: String,
              queryResource: String,
              table: HiveTable,
              overrides: Option[Substitution] = None,
              depends: Set[(Witness => Witness, Goal)] = Set.empty)
         ( implicit ms : MetaStore = null ): Goal = {

      //// Set the jar path 
        ///val hiveSatisfier = new HiveSatisfier(query, HiveDriver("/Users/jeromebanks/NewGit/satisfaction/auxlib"))
        val hiveSatisfier = new HiveSatisfier(queryResource, new  HiveLocalDriver )
        
        val tblVariables = ms.getVariablesForTable(table.dbName, table.tblName)
        val tblOutputs = collection.Set(table)

        new Goal(name = name,
            satisfier = Some(hiveSatisfier),
            variables = tblVariables,
            overrides,
            depends,
            evidence = tblOutputs
        ) with TrackOriented {
           override def setTrack( track : Track ) {
              super.setTrack(track) 
              println(" HiveGoal setTrack")
              val toSatisfier = satisfier.get.asInstanceOf[TrackOriented]
              toSatisfier.setTrack(track)
           }
        }
    }

}