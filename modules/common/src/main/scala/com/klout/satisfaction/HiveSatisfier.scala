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
case class HiveSatisfier(queryTemplate: String, driver: HiveDriver) extends Satisfier {

    val YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd")

    /**
     *  TODO Hive satisfier needs to access
     *    project level properties ...
     *     needs to be in a well known place ...
     */
    def getProperties(witness: Substitution): Substitution = {
        var maxwellProperties: Substitution = Substitution(Substituter.readProperties("maxwell.properties"))

        ///// Some munging logic to translate between camel case 
        //// and  underscores
        ////   and to do some simple date logic

        if (witness.contains(Variable("dt"))) {
            //// convert to Date typed variables... 
            //// not just strings 
            var jodaDate = YYYYMMDD.parseDateTime(witness.get(Variable("dt")).get)
            ////val assign : VariableAssignment[String] = ("dateString" -> YYYYMMDD.print(jodaDate))
            val dateVars = Substitution((Variable("dateString") -> YYYYMMDD.print(jodaDate)),
                (Variable("yesterdayString") -> YYYYMMDD.print(jodaDate.minusDays(1))),
                (Variable("prevdayString") -> YYYYMMDD.print(jodaDate.minusDays(2))),
                (Variable("weekAgoString") -> YYYYMMDD.print(jodaDate.minusDays(7))),
                (Variable("monthAgoString") -> YYYYMMDD.print(jodaDate.minusDays(30))));

            println(s" Adding Date variables ${dateVars.raw.mkString}")
            maxwellProperties = maxwellProperties ++ dateVars

        }

        /// XXX Other domains won't have social networks ...
        if (witness.contains(Variable("network_abbr"))) {
            maxwellProperties = maxwellProperties + (Variable("networkAbbr") -> witness.get(Variable("network_abbr")).get)
            //// needs to be handled outside of satisfier ???
            maxwellProperties = maxwellProperties + (Variable("featureGroup") -> "3")
        }

        maxwellProperties ++ witness

    }

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

        val allProps = getProperties(params)
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
