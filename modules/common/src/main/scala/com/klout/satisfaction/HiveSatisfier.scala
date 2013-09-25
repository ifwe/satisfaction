package com.klout.satisfaction

import hive.ms._
import org.joda.time.format.DateTimeFormat
import org.joda.time.Days

// class HiveSatisfier(ms: MetaStore) extends Satisfier with DataProducing {

//     override def satisfy(goal: Goal, witness: Witness) {
//         if (!goal.isInstanceOf[HiveGoal])
//             throw new IllegalArgumentException("Only HiveGoals are supported")
//     }

// }

// object HiveSatisfier extends HiveSatisfier(MetaStore)

///case class HiveSatisfier(queryTemplate: String, driver: HiveClient) extends Satisfier {
case class HiveSatisfier(queryTemplate: String, driver: HiveDriver) extends Satisfier with ProjectOriented {

    def executeMultipleHqls(hql: String): Boolean = {
        val multipleQueries = hql.split(";")
        multipleQueries.foreach(query => {
            if (query.trim.length > 0) {
                println(s" Executing query $query")
                val results = driver.executeQuery(query)
                if (!results)
                    return results

            }

        })
        true
    }

    def satisfy(params: Substitution): Boolean = {
        println(" Project substitution is as follows " + params.assignments.mkString)

        val allProps = getProjectProperties(params)
        val queryMatch = Substituter.substitute(queryTemplate, allProps) match {
            case Left(badVars) =>
                println(" Missing variables in query Template ")
                badVars.foreach { s => println("  ## " + s) }
                return false
            case Right(query) =>
                try {
                    return executeMultipleHqls(query)
                } catch {
                    case unexpected =>
                        println(s" Unexpected error $unexpected")
                        unexpected.printStackTrace()
                        return false
                }
        }
        println("Fall through to bottom ")
        false
    }
}
