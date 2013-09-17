package com.klout.satisfaction

import hive.ms.MetaStore
import hive.ms.HiveClient
import scala.io.Source
import hive.ms.HiveDriver
import hive.ms.HiveLocalDriver

/**
 */
object HiveGoal {
    implicit lazy val hiveDriver: HiveDriver = new HiveLocalDriver
    ///lazy val hiveClient: HiveClient = HiveClient

    def readResource(fileName: String): String = {
        val resourceUrl = classOf[Goal].getClassLoader().getResource(fileName)
        println(s" Resource URL is $resourceUrl")
        if (resourceUrl == null)
            throw new IllegalArgumentException(s"Resource $fileName not found")
        val readLines = Source.fromURL(resourceUrl).getLines.mkString("\n")
        println(readLines)

        readLines
    }

    def apply(name: String,
              query: String,
              table: HiveTable,
              overrides: Option[Substitution] = None,
              depends: Set[(Witness => Witness, Goal)] = Set.empty): Goal = {

        val hiveSatisfier = new HiveSatisfier(query, hiveDriver)
        val tblVariables = MetaStore.getVariablesForTable(table.dbName, table.tblName)
        val tblOutputs = collection.Set(table)

        new Goal(name = name,
            satisfier = Some(hiveSatisfier),
            variables = tblVariables,
            overrides,
            depends,
            evidence = tblOutputs
        )
    }

}