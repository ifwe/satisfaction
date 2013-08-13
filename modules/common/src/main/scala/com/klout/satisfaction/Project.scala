package com.klout.satisfaction

case class Project(
    name: String,
    topLevelGoals: Set[Goal],
    projectParameters: ParamMap,
    witnessGenerator: WitnessGenerator) {

    lazy val allGoals: Set[Goal] = {
        def allGoals0(toCheck: List[Goal], accum: Set[Goal]): Set[Goal] = {
            toCheck match {
                case Nil => accum
                case current :: remaining =>
                    val currentDeps =
                        if (accum contains current) Nil
                        else current.dependencies map (_._2)

                    allGoals0(remaining ++ currentDeps, accum + current)
            }
        }

        allGoals0(topLevelGoals.toList, Set.empty)
    }

    lazy val internalGoals: Set[Goal] = allGoals filter (_.satisfier.isDefined)

    lazy val externalGoals: Set[Goal] = allGoals filter (_.satisfier.isEmpty)
}