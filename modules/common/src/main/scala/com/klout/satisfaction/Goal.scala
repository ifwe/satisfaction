package com.klout.satisfaction

case class Goal(
    name: String,
    satisfier: Option[Satisfier],
    variables: Set[String] = Set.empty,
    dependencies: Witness => Set[(Goal, Witness)],
    evidence: Set[Evidence])
