package com.klout.satisfaction
package executor
package actors

case object YourProject
case class MyProject(project: Project)

case class AddProject(jarPath: String, name: String)
case class RemoveProject(name: String)
case class GetProject(name: String)
case object GetProjects

case class ProjectResult(project: Option[Project])
case class ProjectList(names: Set[String])

case class AreYouDone(witness: Witness)
case class IAmDone(witness: Witness, goal: Goal)

case object SatisfyGoal
case object GoalSatisfied

case object MaybeGenerateWitness
case class NewWitnessGenerated(witness: Witness)

case object CheckEvidence

