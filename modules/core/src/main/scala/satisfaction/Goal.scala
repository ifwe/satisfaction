package com.klout
package satisfaction

import collection._
import org.joda.time.format.DateTimeFormat
import fs._

case class Goal(
    val name: String,
    val satisfier: Option[Satisfier],
    val variables: Set[Variable[_]] = Set.empty,
    var dependencies: Set[(Witness => Witness, Goal)] = Set.empty,
    ///var evidence: Set[_ <: Evidence] = Set.empty)
    var evidence: Set[Evidence] = Set.empty )
    (implicit val track : Track ) {
  
    locally {
       println(" Creating a Goal !!! ") 
       /// track.add Goal this ...
    }
  
   ///implicit var track : Track = Track.trackForGoal( this)

    def addDependency(goal: Goal): Goal = {
        dependencies += Tuple2(Goal.Identity, goal)
        return this
    }
    
    def addDataDependency( depData : DataOutput) : Goal = {
       addDependency( DataDependency(depData))
       this
    }
    
    ///def addEvidence[E <: Evidence]( ev : E  ) : Goal = {
    def addEvidence( ev : Evidence  ) : Goal = {
       ///evidence += ev
      ///evidence = evidence + ev
      println( "EV is " + ev + " " + ev.getClass())
      println( "Evidence set is "+ evidence + " " + evidence.getClass())
         
      evidence += ev
       this
    }

    def addWitnessRule(rule: (Witness => Witness), goal: Goal): Goal = {
        dependencies += Tuple2(rule, goal)
        return this
    }
    
    
    def addYesterdayGoal( goal : Goal) : Goal = {
        addWitnessRule( Goal.yesterday , goal)      
    }

    def getPredicateString(w: Witness): String = {
        Goal.getPredicateString(this, w)
    }
    
    def declareTopLevel() : Goal = {
      track.addTopLevelGoal(this) 
      this
    }
    
}

object Goal {
    val Identity: (Witness => Witness) = { w: Witness => w }

    def qualifyWitness(param: Variable[String], paramValue: String): (Witness => Witness) = {
        w: Witness =>
            w.update(param, paramValue)
    }
    
    
    /**
     *  Define a Witness mapping function which replaces
     *    the value in the 'dt' variable with a day previous.
     *    
     *  This way we can say that a Goal depends upon another
     *   Goal with yesterday's data    
     */
    def yesterday : ( Witness => Witness) = daysPrevious(1)
    
    def daysPrevious(numDays: Int ) : ( Witness => Witness) = { w: Witness => {
         val dtVar = Variable("dt")
         val yyyymmdd = DateTimeFormat.forPattern("YYYYMMdd")
         if( w.contains( dtVar) ) {
           val dtYMD = w.get( dtVar)
           val yesterDT = yyyymmdd.print( yyyymmdd.parseDateTime(dtYMD.get).minusDays(numDays))
           w.update( dtVar -> yesterDT )
         } else {
           w
         }
      }
    }
    
    
    /**
     *  Replace the variable 'dtVar' with the most recent date in the given path.
     */
    def mostRecentPath( fs : FileSystem , path : Path, dtVar : Variable[String] ) : (Witness => Witness ) = { w: Witness => {
        /// XXX probably want to do some error checking 
        ///  that path exists and is of YYYYMMDD format
        val maxDt = fs.listFiles( path).map(  _.getPath.name ).max
        
        w.update( dtVar -> maxDt )
      }
    }
    

    def stripVariable(param: Variable[_]): (Witness => Witness) = {
        w: Witness =>
        new Witness( w.assignments.filter(!_.variable.equals(param)))
    }

    def getPredicateString(goal: Goal, w: Witness): String = {
        getPredicateString( goal.name, w)
    }
    
    def getPredicateString(goalName: String, w: Witness): String = {
        (goalName + "(" + w.raw.mkString(",") + ")").replace(" ", "").replace("->", "=")
    }
}
