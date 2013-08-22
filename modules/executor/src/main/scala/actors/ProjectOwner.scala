package com.klout.satisfaction
package executor
package actors

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

class ProjectOwner(val project: Project) extends Actor with ActorLogging {

    def receive = {
        case YourProject =>
            sender ! MyProject(project)

        case NewWitnessGenerated(witness) =>
            project.topLevelGoals foreach { goal =>
                println("Asking are you done for goal  " + goal.name + " with id " + goal.uniqueId)
                ///context.actorSelection("./${goal.uniqueId}") ! AreYouDone(witness)
                context.actorSelection(goal.uniqueId) ! AreYouDone(witness)
            }
    }

    override def preStart() {
        val goalRegistry = project.allGoals map { goal =>
            println("Starting goal owner for Goal " + goal.name + " with id " + goal.uniqueId)
            goal -> context.actorOf(Props(new GoalOwner(goal, project.projectParams)), goal.uniqueId)
        } toMap

        ///val witnessGenerator = context.actorOf(Props(new WitnessGeneratorActor(project.witnessGenerator)))

    }

}