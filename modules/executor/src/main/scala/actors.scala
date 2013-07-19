package com.klout.satisfaction
package actors

import common.dsl._

import akka.actor._

object messages {

    case class AreYouSatisfied(goalPeriod: GoalContext)

    case object IAmNotSatisfied

    case object IAmSatisfied

    case class AddProject(name: String, project: Project)

    case class RemoveProject(name: String)

}

import messages._

class ProjectManager extends Actor {

    var projects: Map[String, Project] = Map.empty

    override def receive = {
        case AddProject(name, project) => projects += name -> project

        case RemoveProject(name)       => projects -= name
    }
}

class ProjectOwner extends Actor {
    override def receive = {
        case _ =>
    }
}

class GoalContextOwner(val goalPeriod: GoalContext) extends Actor {
    override def receive = {
        case _ =>
    }
}

class GoalOwner(val goal: InternalGoal) extends Actor {
    override def receive = {
        case AreYouSatisfied(params) => IAmNotSatisfied
    }
}
