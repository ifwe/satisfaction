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
object FanOutGoal {
  /**
   *   Return a Goal which depends upon multiple subGoals,
   *   which a mapping function applied
   */ 
    def apply[T](subGoal: Goal, saturateVar: Variable[T], substSeq:Iterable[T]): Goal = {
        val vars = subGoal.variables.filter( _ != saturateVar )
        implicit val track = subGoal.track
        val deps = substSeq.map(Goal.qualifyWitness(saturateVar, _)).map(Tuple2(_, subGoal))
        new Goal(
            name = "FanOut " + subGoal.name,
            satisfier = None,
            variables = vars,
            dependencies = deps.toSet,
            evidence = Set.empty)
    }


    /**
     *   Allow two sets of variable values to be fanned out.
     */
    def apply(subGoal: Goal, saturateVar: Variable[String], substSeq: List[String],
              saturateVar2: Variable[String], substSeq2: List[String]): Goal = {
        val vars = subGoal.variables.filter( v => { v != saturateVar && v != saturateVar2  })

        implicit val track = subGoal.track
        
        if (substSeq.size != substSeq.size) {
            throw new IllegalArgumentException(" Fan out values must be same size for multiple variables ")
        }
        val qualFuncs = Iterator.range(0, substSeq.size).collect {
            case i: Int => {
                val val1: String = substSeq(i)
                val val2: String = substSeq2(i)
                val qualFunc1 = Goal.qualifyWitness(saturateVar, val1)
                val qualFunc2 = Goal.qualifyWitness(saturateVar2, val2)
                qualFunc1 compose qualFunc2
            }
        }.toSet
        val deps = qualFuncs.map(Tuple2(_, subGoal))

        new Goal(
            name = "FanOut " + subGoal.name,
            satisfier = None,
            variables = vars,
            dependencies = deps,
            evidence = Set.empty)
    }

}
