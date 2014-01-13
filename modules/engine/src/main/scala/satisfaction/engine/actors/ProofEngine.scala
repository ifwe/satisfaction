package com.klout
package satisfaction
package engine
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
    implicit val timeout = Timeout(24 hours)

    /**
     *  Blocking call to satisfy Goal
     */
    def satisfyGoalBlocking(track : Track, goal: Goal, witness: Witness, duration: Duration): GoalStatus = {
        val f = getProver(track,goal, witness) ? Satisfy
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

    def satisfyGoal(track : Track, goal: Goal, witness: Witness): Future[GoalStatus] = {
        future {
            val f = getProver(track, goal, witness) ? Satisfy
            val response = Await.result(f, Duration(6, HOURS))
            response match {
                case s: GoalSuccess =>
                    println(" Goal Was Satisfied")
                    ProverFactory.releaseProver(proverFactory, goal, witness)
                    s.goalStatus
                case f: GoalFailure =>
                    println(" Failure ")
                    f.goalStatus
            }
        }
    }
    
    def restartGoal( track : Track, goal : Goal, witness: Witness ) : Future[GoalStatus] = {
       future {
            val f = getProver(track, goal, witness) ? RestartJob
            val response = Await.result(f, Duration(6, HOURS))
            response match {
                case s: GoalSuccess =>
                    println(" Goal Was Satisfied")
                    ProverFactory.releaseProver(proverFactory, goal, witness)
                    s.goalStatus
                case f: GoalFailure =>
                    println(" Failure ")
                    f.goalStatus
            }
        }
    }


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

    def isSatisfied(track: Track, goal: Goal, witness: Witness): Boolean = {
        getStatus(track, goal, witness).state == GoalState.AlreadySatisfied
    }

    /**
     *  Status should return immediately
     */
    def getStatus(track : Track, goal: Goal, witness: Witness): GoalStatus = {
        val f = getProver(track, goal, witness) ? WhatsYourStatus

        val response = Await.result(f, timeout.duration).asInstanceOf[StatusResponse]
        response.goalStatus
    }

    def getProver(track : Track, goal: Goal, witness: Witness): ActorRef = {
        if( goal.isInstanceOf[TrackOriented]) {
          val to = goal.asInstanceOf[TrackOriented]
          to.setTrack( track)
        }
        ProverFactory.getProver(proverFactory, track, goal, witness)
    }

    def getGoalsInProgress: Set[GoalStatus] = {
        val activeActorsF = proverFactory ? GetActiveActors

        val activeActors = Await.result(activeActorsF, timeout.duration).asInstanceOf[Set[ActorRef]]
        println("Getting Goals in progres ")
        println(" Active actors are " + activeActors)

        /// Get a set of futures for every actor, and ask their status
        val listOfRequests: Set[Future[StatusResponse]] = activeActors.map(ask(_, WhatsYourStatus).mapTo[StatusResponse])
        val futureList = Future.sequence(listOfRequests)
        val fMap = futureList.map(_.map(_.goalStatus))

        ///Await.result( fMap, timeout.duration).asInstanceOf[Set[GoalStatus]
        var resultSet = Await.result(fMap, timeout.duration)
        
        //// Not quite what we want because we only want the ones for a particular track ...
        println(s" REsultSet SIZE is ${resultSet.size} Active Actors = ${activeActors.size} ")
        while( resultSet.size < activeActors.size ) {
           println(" Result Set size is " + resultSet.size)
           resultSet = Await.result(fMap, timeout.duration)
        }
        resultSet.foreach( statResp => {
            println(" Status = " + statResp.goalName + " :: " + statResp.witness + " :: " + statResp.state + " result = " + statResp.execResult)
        })
        resultSet
    }
    
    
    def getGoalStatus( trackName : String, goalName : String ) : Option[GoalStatus] = {
      null  
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
object ProofEngine extends ProofEngine {

    def getActorName(goal: Goal, witness: Witness): String = {
        ///"akka://localhost/satisfaction/" + Goal.getPredicateString(goal, witness).replace("(", "/").replace(",", "/").replace(")", "").replace("=", "_eq_")
        Goal.getPredicateString(goal, witness).replace("(", "_").replace(",", "___").replace(")", "").replace("=", "_eq_")
    }
    def getActorName(goalName: String, witness: Witness): String = {
        Goal.getPredicateString(goalName, witness).replace("(", "_").replace(",", "___").replace(")", "").replace("=", "_eq_")
    }

}