package com.klout.satisfaction

import collection._

case class Goal(
    name: String,
    satisfier: Option[Satisfier],
    variables: Set[Variable[_]] = Set.empty,
    overrides: Option[Substitution] = None,
    var dependencies: Set[(Witness => Witness, Goal)] = Set.empty,
    evidence: Set[Evidence] = Set.empty) {

    ///lazy val uniqueId = java.util.UUID.randomUUID().toString

    def addDependency(goal: Goal): Goal = {
        println(" Dependencies = " + dependencies)
        dependencies += Tuple2(Goal.Identity, goal)
        return this
    }

    def addWitnessRule(rule: (Witness => Witness), goal: Goal): Goal = {
        println(" Dependencies = " + dependencies)
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

    def getPredicateString(goal: Goal, w: Witness): String = {
        (goal.name + "(" + w.substitution.raw.mkString(",") + ")").replace(" ", "").replace("->", "=")
    }
}
