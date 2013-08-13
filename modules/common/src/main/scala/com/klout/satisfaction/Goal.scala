package com.klout.satisfaction

case class Goal(
    name: String,
    satisfier: Option[Satisfier],
    variables: Set[Param[_]] = Set.empty,
    overrides: Option[ParamOverrides],
    dependencies: Set[(Witness => Witness, Goal)],
    evidence: Set[Evidence]) {

    lazy val uniqueId = java.util.UUID.randomUUID().toString
}
