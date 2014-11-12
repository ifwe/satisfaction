package satisfaction


/**
 * XXX TODO
 *    Create multiple dependencies for a goal for a range of values ..
 *    
 *   For now the range needs to be know at compile time, 
 *     or at least track build time.
 *     
 *   Change so that range can be determined at goal satisfy time
 *   ( ie. specify start time and endtime in the witness )     
 *    
 */

/**
 *  DynamicGoals apply Witness function generator 
 */
class DynamicGoal( fName: String, subGoal : Goal,val witnessFunctionGenerator : Witness=>Seq[(Witness=>Witness)] )(implicit track: Track) 
       extends Goal( name=fName, 
                     satisfierFactory = Goal.EmptyFactory,
                     variables = subGoal.variables,
                     dependencies = Set.empty, /// Not used
                     evidence = Set(Evidence.NeverSatisfied) ) {
  
   
   override def hasDependencies =  { true }
   
   override def dependenciesForWitness( w : Witness)  : Seq[(Witness=>Witness,Goal)] = {
     witnessFunctionGenerator( w ).map(  (_,subGoal))
   }
   
}

/**
 *  Create a set of dependencies based on saturating a variable
 *   in the witness with values from a function creating a sequence of values.
 *   
 *   This might be useful for when the values of the subgoals are not known
 *   ahead of time, (i.e. generated on the fly)
 *   
 */
object FanOutGoal {
  /**
   *   Return a Goal which depends upon multiple subGoals,
   *   which a mapping function applied
   */ 
    def apply[T](subGoal: Goal, saturateVar: Variable[T], substSeq: => Seq[T]): Goal = {
        implicit val track = subGoal.track
        //// Scala magic ...
        //// Define a function 
        def fanOutFunction : (Witness=>Seq[(Witness=>Witness)]) = {
           witness : Witness => {
              substSeq.map( Goal.qualifyWitness( saturateVar, _) ).toSeq
           }  
        }
        new DynamicGoal(
            fName = "FanOut " + subGoal.name,
            subGoal,
            fanOutFunction
            )
    }
}

/*
 * Similar to FanOutGoal, but have the Goals depend on 
 *  on another "in sequence", so that only one goal is
 *  being satisfined at a time
 */
object InSequenceGoal {
 
  
    /**
     *  Convert a set of WitnessMappings to one WitnessMapping
     *   which has all it's dependencies chained in a row "in sequence"
     *   ( as compared to in parallel with FanOut )
     */
    def ConvertToInSequence(   fanOut : Seq[((Witness=>Witness),Goal)]) : ((Witness=>Witness),Goal) = {
        fanOut.reduce( (left,right) => {
          val chained  = right._2.addWitnessRule( left._1,left._2)
          val w : Witness = Witness()
          val wl2 = left._1(w)
          val wr2 = right._1(w)
           (right._1,chained )
        } )
    }
    
    
    def apply[T](subGoal: Goal, saturateVar: Variable[T], substSeq: => Seq[T]): Goal = {
           implicit val track = subGoal.track
        //// Scala magic ...
        //// Define a function 
        def fanOutFunction : (Witness=>Seq[(Witness=>Witness)]) = {
           witness : Witness => {
              substSeq.map( Goal.qualifyWitness( saturateVar, _) ).toSeq
           }  
        }
        new DynamicGoal(
            fName = "InSequence " + subGoal.name,
            subGoal,
            fanOutFunction
            ) {
  
            override def dependenciesForWitness( w : Witness)  : Seq[(Witness=>Witness,Goal)] = {
              Seq( ConvertToInSequence( super.dependenciesForWitness( w) ) )
            }
        }
    
    }
  
}
