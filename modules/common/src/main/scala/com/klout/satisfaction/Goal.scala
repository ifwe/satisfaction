package com.klout.satisfaction

case class Goal(
    name: String,
    satisfier: Option[Satisfier],
    variables: Set[Param[_]] = Set.empty,
    overrides: Option[ParamOverrides],
    var dependencies: Set[(Witness => Witness, Goal)],
    evidence: Set[Evidence]) {

    def addDependency(goal: Goal): Goal = {
        dependencies += Tuple2(Goal.Identity, goal)
        return this
    }

    def addWitnessRule(rule: (Witness => Witness), goal: Goal): Goal = {
        dependencies += Tuple2(rule, goal)
        return this
    }
}

object Goal {
    val Identity: (Witness => Witness) = { w: Witness => w }

    def qualifyWitness(param: Param[String], paramValue: String): (Witness => Witness) = {
        w: Witness =>
            val newParam = w.variables.update(param, paramValue)
            new Witness(newParam)
    }
}

