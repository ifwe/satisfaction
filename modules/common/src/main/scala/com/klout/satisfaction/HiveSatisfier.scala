package com.klout.satisfaction

import hive.ms._

// class HiveSatisfier(ms: MetaStore) extends Satisfier with DataProducing {

//     override def satisfy(goal: Goal, witness: Witness) {
//         if (!goal.isInstanceOf[HiveGoal])
//             throw new IllegalArgumentException("Only HiveGoals are supported")
//     }

// }

// object HiveSatisfier extends HiveSatisfier(MetaStore)

case class HiveSatisfier(queryTemplate: String, driver: HiveClient) extends Satisfier {

    def satisfy(params: Substitution): Boolean = {
        val queryMatch = Substituter.substitute(queryTemplate, params) match {
            case Left(badVars) =>
                println(" Missing variables in query Template ")
                badVars.foreach { s => println("  ## " + s) }
                return false
            case Right(query) =>
                try {
                    println(s" Executing query $query")
                    val results = driver.executeQuery(query)
                    return results
                } catch {
                    case unexpected =>
                        println(s" Unexpected error $unexpected")
                        unexpected.printStackTrace()
                        false
                }
        }
        false
    }
}
