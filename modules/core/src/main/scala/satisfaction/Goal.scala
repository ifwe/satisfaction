package satisfaction

import collection._
import org.joda.time.format.DateTimeFormat
import fs._

case class Goal(
    val name: String,
    val satisfierFactory: SatisfierFactory,
    val variables: List[Variable[_]] = List.empty,
    val dependencies: Set[(Witness => Witness, Goal)] = Set.empty,
    val evidence: Set[Evidence] = Set.empty ) 
    (implicit val track : Track ) {
  
    /**
     *  Why is hashcode so slow ???
     *   XXX
     *   TODO make better hash code
     */
    @Override
    override def hashCode = {
      name.hashCode + variables.mkString.hashCode
    }
    
    @Override
    override def toString() = {
      track.descriptor.trackName + name +  variables.mkString
    }
    

    /**
     *  Satisfier need to be a factory.
     *   Goal might have multiple satisfiers running 
     */
    def newSatisfier( w : Witness) : Option[Satisfier] = {
       satisfierFactory(w)
    }

    def addDependency(goal: Goal): Goal = {
      copy( dependencies = dependencies + Tuple2(Goal.Identity,goal))
    }
    
    def addDataDependency( depData : DataOutput) : Goal = {
       addDependency( DataDependency(depData))
    }
    
    def dependentGoals() :  Seq[Goal] = {
       dependencies.map( _._2).toSeq
    }
      
    
    ///def addEvidence[E <: Evidence]( ev : E  ) : Goal = {
    def addEvidence( ev : Evidence  ) : Goal = {
       copy( evidence = evidence + ev)
    }
    
    def hasEvidence : Boolean = {
      evidence.size > 0
    }
    
    def evidenceForWitness( w : Witness) : Seq[Evidence] = {
      evidence.toSeq
    }
    
    def hasDependencies : Boolean = {
      dependencies.size > 0
    }
    
    def dependenciesForWitness( w : Witness ) : Seq[(Witness=>Witness,Goal)] = {
   	  dependencies.toSeq
    }

    /**
     *  Add A Dependency rule for with an explicit Witness mapping function,
     *    so that one can depend upon a Goal satisfied with a different set 
     *     of witnesses.
     */
    def addWitnessRule(rule: (Witness => Witness), goal: Goal): Goal = {
      copy( dependencies =  dependencies +  Tuple2(rule, goal) )
    }
    
    /**
     *  Depend upon yesterday's output of the specified Goal
     */
    def addYesterdayGoal( goal : Goal) : Goal = {
        addWitnessRule( Goal.yesterday , goal)      
    }

    /**
     *  Depend upon the output from a Goal
     *   from the previous hour.
     */
    def addPreviousHourGoal( goal : Goal) : Goal = {
        addWitnessRule( Goal.previousHour , goal)      
    }
    
    
    /**
     *  Add dependencies on all previous days output of this Goal,
     *   so that all unsatisfied output will be generated.
     */
    def reharvestDaily() : Goal = {
       addYesterdayGoal( this) 
    }

    /**
     *  Add dependencies on all previous hours of this Goal,
     *    so that all unsatisfied output will be generated.
     */
    def reharvestHourly() : Goal = {
       addPreviousHourGoal( this) 
    }

    def getPredicateString(w: Witness): String = {
        Goal.getPredicateString(this, w)
    }
    
    def declareTopLevel() : Goal = {
      track.addTopLevelGoal(this) 
      this
    }
    
   
    /*
     * Add multiple dependencies to the goal, substituting the variable with values 
     *    from the Traversable
     */
    def foldWitnessRules[T]( prevRule : ( Witness=>Witness) ,subGoal :Goal, foldVar : Variable[T], valIter : Traversable[T]) : Goal = {
       valIter.foldLeft(  this)( (g : Goal,vv : T) => {
           val witnessMapping : ( Witness=>Witness)  = {
              w => {
               w  + VariableAssignment( foldVar , vv)
             }
           }
           g.addWitnessRule( prevRule compose witnessMapping, subGoal)
       })
    }
    def foldDependencies[T]( subGoal :Goal, foldVar : Variable[T], valIter : Traversable[T]) : Goal = {
      foldWitnessRules(  Goal.Identity, subGoal,foldVar, valIter)
    }
    
    
    /**
     *  Add multiple dependencies to the goal, similiar to fold,
     *   but add them in sequence, so that they are done one at 
     *    a time.
     */
    def chainWitnessRules[T]( prevRule : ( Witness=>Witness) ,subGoal :Goal, foldVar : Variable[T], valIter : Traversable[T]) : Goal = {
      val first = valIter.head
      println(s" HEAD is $first ")

      val chained : Goal = valIter.drop(1).foldLeft( subGoal )( (g: Goal, vv : T) => {
           val witnessMapping : ( Witness=>Witness)  = Goal.qualifyWitness( foldVar, vv )
           println(s" ${g.name} CHAIN WITNESS RULE $vv  ")
           Goal(subGoal).addWitnessRule( witnessMapping,g)
      } )
      //// apply the composed rule onto the first value, and then 
      addWitnessRule( prevRule compose Goal.qualifyWitness(foldVar, first) , chained)
    }

    def chainDependencies[T]( subGoal :Goal, foldVar : Variable[T], valIter : Traversable[T]) : Goal = {
      chainWitnessRules(  Goal.Identity, subGoal,foldVar, valIter)
    }
    
    /**
     *  Depend upon N previous hours of the specified Goal
     */
    def forPreviousHours( subGoal : Goal, numHours : Int ) : Goal = {
      (1 to numHours).foldLeft( this)( (g,nh) => {
          g.addWitnessRule( Goal.hoursPrevious( nh), subGoal)   
      }) 
    }
    
    /**
     *  Depend upon the N previous days of the specified Goal
     *  
     */
    def forPreviousDays( subGoal : Goal, numDays : Int) : Goal = {
      (1 to numDays).foldLeft( this)( (g,nd) => {
          g.addWitnessRule( Goal.daysPrevious( nd), subGoal)   
      }) 
    }
    
} 
object Goal {
  
  
    /**
     *  Copy Constructor 
     */
    def apply( g : Goal )( implicit track : Track) : Goal = {
       new Goal( g.name,
    		   	g.satisfierFactory,
    		   	g.variables,
    		   	g.dependencies,
    		   	g.evidence ) 
    }
    
    /**
     *  Create a SatisfierFactory which returns a specific Satisfier
     */
    def SatisfierFactory( sat : Satisfier)  : SatisfierFactory = {
      { w => {  Some(sat) } }
    }
    
    /**
     *  Given a function which returns a Satisfier, 
     *   Create a SatisfierFactory
     */
    def SatisfierFactoryFromFunc( satFunc : () => Satisfier) : SatisfierFactory = {
      { w => { Some(satFunc()) } }
    }
    
    /**
     *  NoneFactory implies that we wait for data dependency
     */
    val NoneFactory : SatisfierFactory  = {
      { w => { None } }
    }

    /**
     *  EmptyFactory produces EmptySatisfier, which does nothing
     */
    val EmptyFactory : SatisfierFactory  = {
      { w => { SomeEmptySatisfier } }
    }

    /**
     *  Identify function for Witness mapping
     */
    val Identity: (Witness => Witness) = { w: Witness => w }

    /**
     *  Add a variable and value to  a witness
     */
    def qualifyWitness[T](param: Variable[T], paramValue : T): (Witness => Witness) = {
        w: Witness => {
          if(w.contains(param) ) {
            w.update  (param, paramValue)
          } else {
            println(s" ADDing $paramValue to ${param.name} in witness $w  ")
            w + ( param -> paramValue )
          }
        }
    }
    
    
    val  EmptySatisfier : Satisfier = new Satisfier {
        override def name = "DoNothing"
        override def satisfy( w: Witness )  =  robustly {
           true
        }
        override def abort() = robustly {
          true
        }
    }
    val SomeEmptySatisfier = Some(EmptySatisfier)

    
    /**
     *  Define a Witness mapping function which replaces
     *    the value in the 'dt' variable with a day previous.
     *    
     *  This way we can say that a Goal depends upon another
     *   Goal with yesterday's data    
     */
    def yesterday : ( Witness => Witness) = daysPrevious(1)
    
    /**
     *  Map a witness for the output from numDays ago.
     */
    def daysPrevious(numDays: Int )(implicit dateVarName : String = "dt") : ( Witness => Witness) = { w: Witness => {
         val dtVar = Variable(dateVarName)
         val yyyymmdd = DateTimeFormat.forPattern("YYYYMMdd")
         if( w.contains( dtVar) ) {
           val dtYMD = w.get( dtVar)
           val yesterDT = yyyymmdd.print( yyyymmdd.parseDateTime(dtYMD.get).minusDays(numDays))
           val yester  = w.update( dtVar -> yesterDT )
           println( " YEster i = " + yester)

           yester
         } else {
           w
         }
      }
    }
    
    /**
     * Return a Goal which satisfies another goal
     *  according to witness variable mapping,
     *  so that one can satisfy top-level goals 
     *  with a mapped witness
     */
    def MappedGoal( goal : Goal )( mapping : ( Witness => Witness))
        (implicit track : Track) : Goal = {
       new Goal(
           name = s"Mapped${goal.name}",
           satisfierFactory =  SatisfierFactory(EmptySatisfier),
           dependencies = Set( ( mapping, goal) ),
           variables = goal.variables
       ) 
    }
    
    
    /**
     * Declare that this Goal should be satisfied for the previous hour,
     *   rather than the current hour,
     *  (Useful for processing results at the beginning of an hour, using
     *     data gathered in the last hour)
     */
    def ForPreviousHour( goal : Goal )(implicit track: Track) : Goal = MappedGoal(goal) ( previousHour)
    def ForPreviousHours( goal : Goal, numHours : Int)(implicit track: Track) : Goal = MappedGoal(goal) ( hoursPrevious( numHours ))

    def ForPreviousDay( goal : Goal )(implicit track: Track) : Goal = MappedGoal(goal) ( yesterday)
    def ForPreviousDays( goal : Goal, numDays : Int)(implicit track: Track) : Goal = MappedGoal(goal) ( daysPrevious( numDays))

    /**
     *  Define a witness mapping, replacing temporal variables "dt" and "hour"
     *   with values for the previous hour.
     */
    def previousHour : ( Witness => Witness ) = hoursPrevious( 1 )
    
    /**
     *  Define a witness mapping which replaces
     *    dt and hour to be for some previous hours
     */
    def hoursPrevious(numHours: Int )(implicit dateVarName : String = "dt",hourVarName : String = "hour") : ( Witness => Witness) = { w: Witness => {
         val dtVar = Variable(dateVarName)
         val hourVar = Variable(hourVarName)
         val yyyymmddhh = DateTimeFormat.forPattern("YYYYMMddHH")
         val yyyymmdd = DateTimeFormat.forPattern("YYYYMMdd")
         val hh = DateTimeFormat.forPattern("HH")
         if( w.contains( dtVar) && w.contains(hourVar) ) {
           val dtYMD = w.get( dtVar).get
           val dtHH : String = w.get( hourVar).get
           val prevTime = yyyymmddhh.parseDateTime( dtYMD + dtHH).minusHours( numHours)
           val yesterDT = yyyymmdd.print( prevTime)
           val yesterHH = hh.print( prevTime)
           w.update( dtVar -> yesterDT ).update( hourVar -> yesterHH )
         } else {
           w
         }
      }
    }
    
    /**
     *  Return a Witness mapping function 
     *    which increments a variable 
     */
    def incrementVariable( incrVar : Variable[String],  numIncr : Int ) : ( Witness => Witness) = { w: Witness => {
         if( w.contains( incrVar)) {
           val currVal : Int = Integer.parseInt( w.get( incrVar).get)
           w.update( incrVar -> (currVal + numIncr).toString )
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
      //// Also do for hourly
        val maxDt = fs.listFiles( path).map(  _.path.name ).max
        
        w.update( dtVar -> maxDt )
      }
    }
    

    def stripVariable(param: Variable[_]): (Witness => Witness) = {
        w: Witness =>
        new Witness( w.assignments.filter(!_.variable.equals(param)))
    }

    def getPredicateString(goal: Goal, w: Witness): String = {
        getPredicateString( goal.track.descriptor.trackName + "_" + goal.name, w)
    }
    
    def getPredicateString(goalName: String, w: Witness): String = {
        (goalName + "(" + w.mkString(",") + ")").replaceAll(" ", "").replaceAll("=>", "=")
    }
   
       
   /**
    *   Define a function which maps one variable to another.
    *   Useful for Goal dependencies, if one table refers to 
    *    a variable by a different name.
    */ 
    def mapVariables[T]( vfrom : Variable[T], vto : Variable[T] )( fromWitness : Witness) : Witness = {
      val oldValCheck = fromWitness.get( vfrom)
      oldValCheck match {
        case Some(oldVal) => {
          fromWitness.exclude( Set(vfrom)) + ( vto, oldVal)
        } 
        case None => fromWitness
      }
   }

   /**
    *  Define a function which adds a predetermined Witness 
    *    substitution
    */
   def addWitness( constWitness: Witness )( fromWitness : Witness) : Witness = {
       fromWitness ++ constWitness
   }
    
   
   /**
    *  Convenience method for running arbitrary blocks of code
    */
   def RunThis( runName: String,  codeBlock : (Witness=> Boolean) )(implicit track : Track) : Goal = {
     val runThisSatisfier= new Satisfier {
         override def name = runName
         override def satisfy( w : Witness) = robustly { codeBlock( w) }
         override def abort() = {  null } 
     }
     
     new Goal( name = runName,
               satisfierFactory = SatisfierFactory( runThisSatisfier)
          ) (  track )
     
   }
    
}
