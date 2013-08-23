package com.klout.satisfaction

import hive.ms._

// class HiveSatisfier(ms: MetaStore) extends Satisfier with DataProducing {

//     override def satisfy(goal: Goal, witness: Witness) {
//         if (!goal.isInstanceOf[HiveGoal])
//             throw new IllegalArgumentException("Only HiveGoals are supported")
//     }

// }

// object HiveSatisfier extends HiveSatisfier(MetaStore)

class HiveSatisfier(query: String, ms: MetaStore) extends Satisfier {

    def satisfy(params: Substitution): Boolean = {
        true
    }

    def getVariablesForTable() = {

    }

    def substituteProperties(queryTemplate: String, props: Map[String, String]): String = {
        null
    }
}