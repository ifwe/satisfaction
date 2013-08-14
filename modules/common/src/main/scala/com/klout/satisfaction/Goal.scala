package com.klout.satisfaction

case class Goal(
    name: String,
    satisfier: Option[Satisfier],
    variables: Set[Param[_]] = Set.empty,
    overrides: Option[ParamOverrides],
    var dependencies: Set[(Witness => Witness, Goal)],
    evidence: Set[Evidence]) {

    val Id: (Witness => Witness) = { w: Witness => w }

    def addDependency(goal: Goal) {
        dependencies += Tuple2(Id, goal)
    }

}
