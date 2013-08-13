package com.klout.satisfaction
package executor
package actors

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

class ProjectOwner(project: Project) extends Actor with ActorLogging {

    def receive = {
        case YourProject =>
            sender ! MyProject(project)

        case NewWitnessGenerated(witness) =>
            project.topLevelGoals foreach { goal =>
                context.actorSelection("./${goal.uniqueId}") ! AreYouDone(witness)
            }
    }

    override def preStart() {
        val goalRegistry = project.allGoals map { goal =>
            goal -> context.actorOf(Props(new GoalOwner(goal, project.projectParams)), goal.uniqueId)
        } toMap

        val witnessGenerator = context.actorOf(Props(new WitnessGeneratorActor(project.witnessGenerator)))

    }

}