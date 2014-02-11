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
    def satisfyGoalBlocking( goal: Goal, witness: Witness, duration: Duration): GoalStatus = {
        val f = getProver(goal, witness) ? Satisfy
        val response = Await.result(f, duration)
        response match {
            case s: GoalSuccess =>
                println(" Goal Was Satisfied")
                s.goalStatus
            case f: GoalFailure =>
                println(" Received GoalFailure ")
                f.goalStatus
        }
    }

    def satisfyGoal( goal: Goal, witness: Witness): Future[GoalStatus] = {
        future {
            val f = getProver(goal, witness) ? Satisfy
            val response = Await.result(f, Duration(6, HOURS))
            response match {
                case s: GoalSuccess =>
                    println(" Goal Was Satisfied")
                    ProverFactory.releaseProver(proverFactory, goal, witness)
                    s.goalStatus
                case f: GoalFailure =>
                    println(" Received GoalFailure ")
                    f.goalStatus
            }
        }
    }
    
    def restartGoal( goal : Goal, witness: Witness ) : Future[GoalStatus] = {
       future {
            val f = getProver( goal, witness) ? RestartJob
            val response = Await.result(f, Duration(6, HOURS))
            response match {
                case s: GoalSuccess =>
                    println(" Restart Goal Was Successfull" )
                    ProverFactory.releaseProver(proverFactory, goal, witness)
                    s.goalStatus
                case f: GoalFailure =>
                    println(" Restart Failure ")
                    f.goalStatus
            }
        }
    }
    
    def abortGoal( goal : Goal, witness: Witness) : Future[GoalStatus] = {
      future {
         val f = getProver( goal, witness ) ? Abort
         val response = Await.result( f, Duration( 6, HOURS))
         response match {
            case s: GoalSuccess =>
               println(" Abort was successful ")
               ProverFactory.releaseProver(proverFactory, goal, witness)
               s.goalStatus
            case f: GoalFailure =>
               println(" Failure to Abort -- releasing anyway  ")
               ProverFactory.releaseProver(proverFactory, goal, witness)
               f.goalStatus
         }
      }
    }

    def isSatisfied( goal: Goal, witness: Witness): Boolean = {
        getStatus( goal, witness).state == GoalState.AlreadySatisfied
    }

    /**
     *  Status should return immediately
     */
    def getStatus( goal: Goal, witness: Witness): GoalStatus = {
        val f = getProver(goal, witness) ? WhatsYourStatus

        val response = Await.result(f, timeout.duration).asInstanceOf[StatusResponse]
        response.goalStatus
    }

    def getProver(goal: Goal, witness: Witness): ActorRef = {
        ProverFactory.getProver(proverFactory, goal.track, goal, witness)
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
        Goal.getPredicateString(goal, witness).replace("(", "_").replace(",", "___").replace(")", "").replace("=", "_eq_").replace("$","_dlr_").
           replace("{","_lp_").replace("}","_rp_").replace("/","_sl_")
    }
    def getActorName(goalName: String, witness: Witness): String = {
        Goal.getPredicateString(goalName, witness).replace("(", "_").replace(",", "___").replace(")", "").replace("=", "_eq_").replace("$","_dlr_").
           replace("{","_lp_").replace("}","_rp_").replace("/","_sl_")
    }

}