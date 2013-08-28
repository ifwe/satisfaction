package com.klout.satisfaction
package executor
package actors

import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.Future._
import scala.concurrent._
import akka.actor.Props
import akka.pattern.ask
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.EmptyLocalActorRef
import scala.concurrent.Future
import ExecutionContext.Implicits.global

class ProofEngine {

    val akkaSystem = ActorSystem("satisfaction")
    val proverFactory = akkaSystem.actorOf(Props[ProverFactory], "ProverFactory")

    /**
     *  Blocking call to satisfy Goal
     */
    def satisfyGoalBlocking(goal: Goal, witness: Witness, duration: Duration): GoalStatus = {
        val f = getProver(goal, witness) ? Satisfy
        val response = Await.result(f, duration)
        response match {
            case s: GoalSuccess =>
                println(" Goal Was Satisfied")
                s.goalStatus
            case f: GoalFailure =>
                println(" Failure ")
                f.goalStatus
        }
    }

    def satisfyGoal(goal: Goal, witness: Witness): Future[GoalStatus] = {
        future {
            val f = getProver(goal, witness) ? Satisfy
            val response = Await.result(f, Duration(6, HOURS))
            response match {
                case s: GoalSuccess =>
                    println(" Goal Was Satisfied")
                    s.goalStatus
                case f: GoalFailure =>
                    println(" Failure ")
                    f.goalStatus
            }
        }

    }

    implicit val timeout = Timeout(24 hours)

    /**
     * def satisfyProject(project: Project, witness: Witness): Boolean = {
     *
     * val props = Props(new ProjectOwner(project))
     * val actorRef = akkaSystem.actorOf(props, project.name)
     *
     * val future = actorRef ? new NewWitnessGenerated(witness)
     *
     * val result = Await.result(future, timeout.duration).asInstanceOf[Boolean]
     *
     * result
     * }
     *
     */

    def isSatisfied(goal: Goal, witness: Witness): Boolean = {
        getStatus(goal, witness).state == GoalState.AlreadySatisfied
    }

    /**
     *  Status should return immediately
     */
    def getStatus(goal: Goal, witness: Witness): GoalStatus = {
        val f = getProver(goal, witness) ? WhatsYourStatus

        val response = Await.result(f, timeout.duration).asInstanceOf[StatusResponse]
        response.goalStatus
    }

    def getProver(goal: Goal, witness: Witness): ActorRef = {
        ProverFactory.getProver(proverFactory, goal, witness)
    }

    /// sic ....
    //// Want to be able to access by actorFor
    /**
     * var actorFor = akkaSystem.actorFor( goal.getPredicateString(witness))
     * actorFor match {
     * case a : EmptyLocalActorRef =>
     * akkaSystem.actorOf( Props( new PredicateProver( goal,witness)))
     * case _ => actorFor
     * }
     * **
     */

    def stop() {
        akkaSystem.shutdown
    }

}
object ProofEngine {

    def getActorName(goal: Goal, witness: Witness): String = {
        ///"akka://localhost/satisfaction/" + Goal.getPredicateString(goal, witness).replace("(", "/").replace(",", "/").replace(")", "").replace("=", "_eq_")
        Goal.getPredicateString(goal, witness).replace("(", "_").replace(",", "___").replace(")", "").replace("=", "_eq_")
    }

}