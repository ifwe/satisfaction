package com.klout.satisfaction

/**
 *  Factory methods to
 */
object HiveGoalFactory {

    /**
     *  Load from the file, and
     */
    def fromFile(fileName: String): Goal = ???

    def fromQuery(query: String): Goal = ???

    /**
     *  Examine ,
     */
    def forTable(tbl: HiveTable, query: String): Goal = {
        null
    }
    def forTableFromFile(name: String, tbl: HiveTable, fileName: String): Goal = {
        null
    }

}

/**
 *  Simple HiveGoal, where we know what the dependencies are ahead of time.
 */
class HiveGoal(val ms: hive.ms.MetaStore) {
    def apply(name: String,
              hqlString: String,
              depends: Set[(Witness => Witness, Goal)],
              outputs: Set[DataOutput]): Goal = {
        val hiveSatisfier = new HiveSatisfier(hqlString, ms)

        null
    }
}