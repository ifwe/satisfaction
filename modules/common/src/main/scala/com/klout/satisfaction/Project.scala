package com.klout.satisfaction

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

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
     var trackProperties : Substitution = null

}

object Track extends Track("EmptyTrack",Set.empty) {
  
}

trait TemporalVariable {
  
    def getObjectForTime( dt : DateTime) : Any
}


trait TrackOriented {

    val YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd")
    
    var track : Track = null

    def trackName = track.name
      
      
      
    def setTrack( track : Track ) = {
    	this.track = track
    }

    def getTrackProperties(witness: Substitution): Substitution = {
        var projProperties : Substitution =  track.trackProperties

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
            projProperties = projProperties ++ dateVars

        }

        /// XXX Other domains won't have social networks ...
        if (witness.contains(Variable("network_abbr"))) {
            projProperties = projProperties + (Variable("networkAbbr") -> witness.get(Variable("network_abbr")).get)
            //// needs to be handled outside of satisfier ???
            projProperties = projProperties + (Variable("featureGroup") -> "3")
        }

        projProperties ++ witness

    }
}
