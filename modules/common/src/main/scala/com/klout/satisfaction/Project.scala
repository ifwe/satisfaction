package com.klout.satisfaction

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
///import sbt.BuildDependencies

case class Track(
    name: String,
    topLevelGoals: Set[Goal] ) {

    lazy val allGoals: Set[Goal] = {
        def allGoals0(toCheck: List[Goal], accum: Set[Goal]): Set[Goal] = {
            toCheck match {
                case Nil => accum
                case current :: remaining =>
                    val currentDeps =
                        if (accum contains current) Nil
                        else current.dependencies map (_._2)

                    allGoals0(remaining ++ currentDeps, accum + current)
            }
        }

        allGoals0(topLevelGoals.toList, Set.empty)
    }

    lazy val internalGoals: Set[Goal] = allGoals filter (_.satisfier.isDefined)

    lazy val externalGoals: Set[Goal] = allGoals filter (_.satisfier.isEmpty)
    
    
    def getWitnessVariables : Set[Variable[_]] = {
      topLevelGoals.flatMap( _.variables ).toSeq.distinct.toSet
    }
    
    /**
     *  Attach a set 
     *   of properties along with the Track
     * 
     */
     var projectProperties : collection.mutable.Map[String,String] = new collection.mutable.HashMap[String,String]

}

trait TemporalVariable {
  
    def getObjectForTime( dt : DateTime) : Any
}


trait ProjectOriented {

    val YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd")

    def projectName = "maxwell"

    def getProjectProperties(witness: Substitution): Substitution = {
        var maxwellProperties: Substitution = Substitution(Substituter.readProperties(s"${projectName}.properties"))

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
}
