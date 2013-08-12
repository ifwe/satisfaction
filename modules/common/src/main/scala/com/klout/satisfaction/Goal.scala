package com.klout.satisfaction

case class Goal(
    name: String,
    satisfier: Option[Satisfier],
    variables: Set[Param[_]] = Set.empty,
    overrides: ParamOverrides,
    dependencies: Witness => Set[(Goal, Witness)],
    evidence: Set[Evidence])
