package com.klout.satisfaction

class HiveGoal(name: String,
               val hive_file : String,
               variables: Set[String],
               dependencies: PartialFunction[Witness,Set[Tuple2[Goal,Witness]]],
               output: Set[DataOutput])
    extends Goal(name, HiveSatisfier, variables, dependencies, output) {

    def getHiveQuery: String = {
        null
    }

    def getHiveFile: String = {
        hive_file
    }

}

/**
 *  Factory methods to
 */
object HiveGoalFactory {

    def fromFile(fileName: String): HiveGoal = {
        null
    }

    def fromQuery(query: String): HiveGoal = {
        null
    }

}