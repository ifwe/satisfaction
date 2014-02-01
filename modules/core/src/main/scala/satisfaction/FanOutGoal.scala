package com.klout.satisfaction

import com.klout.satisfaction._

object FanOutGoal {
  /**
   *  XXX 
   *   XXXX Think if we want to have distinct 
   *      goals for this, or a custom witness mapper function
   */ 

    def apply(subGoal: Goal, saturateVar: Variable[String], substSeq: Set[String]): Goal = {
        val vars = subGoal.variables - saturateVar
        implicit val track = subGoal.track
        val deps = substSeq.map(Goal.qualifyWitness(saturateVar, _)).map(Tuple2(_, subGoal))
        new Goal(
            name = "FanOut " + subGoal.name,
            satisfier = None,
            variables = vars,
            dependencies = deps,
            evidence = Set.empty)
        ///( subGoal.track)
    }

    /**
     *   Allow two sets of variable values to be fanned out.
     */
    def apply(subGoal: Goal, saturateVar: Variable[String], substSeq: List[String],
              saturateVar2: Variable[String], substSeq2: List[String]): Goal = {
        val vars = subGoal.variables - saturateVar - saturateVar2

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
            ///(subGoal.track)
    }

}