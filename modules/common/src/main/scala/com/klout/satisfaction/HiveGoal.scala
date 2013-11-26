package com.klout.satisfaction

import hive.ms.MetaStore
import hive.ms.HiveClient
import scala.io.Source
import hive.ms.HiveDriver
import hive.ms.HiveLocalDriver

/**
 */
object HiveGoal {


    def apply(name: String,
              queryResource: String,
              table: HiveTable,
              overrides: Option[Substitution] = None,
              depends: Set[(Witness => Witness, Goal)] = Set.empty): Goal = {

      //// Set the jar path 
        ///val hiveSatisfier = new HiveSatisfier(query, HiveDriver("/Users/jeromebanks/NewGit/satisfaction/auxlib"))
        val hiveSatisfier = new HiveSatisfier(queryResource, new  HiveLocalDriver )
        
        val tblVariables = MetaStore.getVariablesForTable(table.dbName, table.tblName)
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
              val toSatisfier = satisfier.get.asInstanceOf[TrackOriented]
              toSatisfier.setTrack(track)
           }
        }
    }

}