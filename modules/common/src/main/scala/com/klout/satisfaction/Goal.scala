package com.klout.satisfaction

import collection._

case class Goal(
    val name: String,
    val satisfier: Option[Satisfier],
    val variables: Set[Variable[_]] = Set.empty,
    var overrides: Option[Substitution] = None,
    var dependencies: Set[(Witness => Witness, Goal)] = Set.empty,
    evidence: Set[_ <: Evidence] = Set.empty) {

    def addDependency(goal: Goal): Goal = {
        dependencies += Tuple2(Goal.Identity, goal)
        return this
    }
    
    def addDataDependency( depData : DataOutput) : Goal = {
       dependencies += Tuple2( Goal.Identity, Goal(s"Data ${depData.toString} ", null, depData.variables))  
       this
    }

    def addWitnessRule(rule: (Witness => Witness), goal: Goal): Goal = {
        dependencies += Tuple2(rule, goal)
        return this
    }

    def getPredicateString(w: Witness): String = {
        Goal.getPredicateString(this, w)
    }

    
}

object Goal {
    val Identity: (Witness => Witness) = { w: Witness => w }

    def qualifyWitness(param: Variable[String], paramValue: String): (Witness => Witness) = {
        w: Witness =>
            val newParam = w.substitution.update(param, paramValue)
            new Witness(newParam)
    }

    def stripVariable(param: Variable[_]): (Witness => Witness) = {
        w: Witness =>
            val withoutParam = w.substitution.assignments.filter(!_.variable.equals(param))
            new Witness(new Substitution(withoutParam))
    }

    def getPredicateString(goal: Goal, w: Witness): String = {
        getPredicateString( goal.name, w)
    }
    
    def getPredicateString(goalName: String, w: Witness): String = {
        (goalName + "(" + w.substitution.raw.mkString(",") + ")").replace(" ", "").replace("->", "=")
    }
}
