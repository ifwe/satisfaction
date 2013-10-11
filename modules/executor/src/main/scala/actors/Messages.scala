package com.klout.satisfaction
package executor
package actors

case class AddProject(jarPath: String, name: String)
case class RemoveProject(name: String)
case class GetProject(name: String)
case object GetProjects

case class ProjectResult(project: Option[Track])
case class ProjectList(names: Set[String])

case object CheckEvidence
case object GoalSatisfied
case object GoalFailed
