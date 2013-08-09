package com.klout.satisfaction

case class Project(
    name: String,
    topLevelGoals: Set[Goal],
    projectParameters: Map[String, String],
    witnessGenerator: WitnessGenerator)