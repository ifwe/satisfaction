package com.klout.satisfaction

case class Goal(
    name: String,
    satisfier: Option[Satisfier],
    variables: Set[Param[_]] = Set.empty,
    overrides: Option[ParamOverrides],
    var dependencies: Set[(Witness => Witness, Goal)],
    evidence: Set[Evidence]) {

    lazy val uniqueId = java.util.UUID.randomUUID().toString

    def addDependency(goal: Goal): Goal = {
        dependencies += Tuple2(Goal.Identity, goal)
        return this
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

    def qualifyWitness(param: Param[String], paramValue: String): (Witness => Witness) = {
        w: Witness =>
            val newParam = w.params.update(param, paramValue)
            new Witness(newParam)
    }

    def getPredicateString(goal: Goal, w: Witness): String = {
        (goal.name + "(" + w.params.raw.mkString(",") + ")").replace(" ", "").replace("->", "=")
    }
}
