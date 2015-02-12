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
import satisfaction.track.TrackHistory
import org.joda.time.DateTime
import satisfaction.notifier.Notified
import satisfaction.notifier.Notifier
import akka.actor.Actor
import akka.actor.ActorLogging
import satisfaction.retry.Retryable
import akka.actor.DeadLetter
import com.typesafe.config.ConfigFactory

class ProofEngine( val trackHistoryOpt : Option[TrackHistory] = None) extends  satisfaction.Logging{

    implicit val config : com.typesafe.config.Config = ConfigFactory.load
    implicit val akkaSystem = ActorSystem("satisfaction", config ,this.getClass.getClassLoader)
    val proverFactory = {
       val actorRef = akkaSystem.actorOf(Props( classOf[ProverFactory], trackHistoryOpt), "ProverFactory")
       akkaSystem.eventStream.subscribe(actorRef, classOf[DeadLetter])
       actorRef
    }
    implicit val timeout = Timeout(60 seconds) /// Configure !!!!
    
    
    

    /**
     *  Blocking call to satisfy Goal
     */
    def satisfyGoalBlocking( goal: Goal, witness: Witness, duration: Duration): GoalStatus = {
        val f = getProver(goal, witness) ? Satisfy(true)
        val response = Await.result(f, duration)
        response match {
            case s: GoalSuccess =>
                info(s" Goal ${goal.name} was Satisfied")
                ProverFactory.releaseProver(proverFactory, goal, witness)
                s.goalStatus
            case f: GoalFailure =>
                info(s" Goal ${goal.name} received GoalFailure ")
                f.goalStatus
        }
    }
    
    def satisfyGoal( goal: Goal, witness: Witness): Future[GoalStatus] = {
        future {
            val f = getProver(goal, witness) ? Satisfy(true)
            val response = Await.result(f, Duration(6, HOURS)) /// XXX Allow for really long jobs ... put in config somehow ..
            response match {
                case s: GoalSuccess =>
                    info(s" Goal ${goal.name} Was Satisfied")
                    ProverFactory.releaseProver(proverFactory, goal, witness)
                    s.goalStatus
                case f: GoalFailure =>
                    info(s" Goal ${goal.name} received GoalFailure ")
                    f.goalStatus
            }
       }
    }
    
    def restartGoal( goal : Goal, witness: Witness ) : Future[GoalStatus] = {
       future {
            info(s" Restarting Goal ${goal.name} ( ${witness} )")
            val f = getProver( goal, witness) ? RestartJob()
            val response = Await.result(f, Duration(6, HOURS))
            response match {
                case s: GoalSuccess =>
                    info(s" Restart Goal ${goal.name} was Successfull" )
                    ProverFactory.releaseProver(proverFactory, goal, witness)
                    s.goalStatus
                case f: GoalFailure =>
                    info(s" Restart Goal ${goal.name} was Failure ")
                    f.goalStatus
            }
        }
    }
    
    ///def abortGoal( goal : Goal, witness: Witness) : Future[GoalStatus] = {
    /// XXX JDB Abort is fire and forget for now 
    def abortGoal( goal : Goal, witness: Witness) : Unit = {
      ///future {
         val prover = getProver( goal, witness ) 
         info(" Prover is " + prover)
         println(" Prover is " + prover)
         prover ! Abort(killChildren=true)
         info(" Prover message sent  " + prover)
         println(" Prover message sent  " + prover)
         /**
         val response = Await.result( f, Duration( 6, HOURS))
         response match {
            case s: GoalSuccess =>
               info(s" Abort Goal ${goal.name} was successful ")
               ProverFactory.releaseProver(proverFactory, goal, witness)
               s.goalStatus
            case f: GoalFailure =>
               info(s" Failure to Abort Goal ${goal.name} -- releasing anyway  ")
               ProverFactory.releaseProver(proverFactory, goal, witness)
               f.goalStatus
         }
         */
      ///}
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
      ProverFactory.acquireProver(proverFactory, goal.track, goal, witness)
    }

    def getGoalsInProgress: Set[GoalStatus] = {
        val activeActorsF = proverFactory ? GetActiveActors

        val activeActors = Await.result(activeActorsF, timeout.duration).asInstanceOf[Set[ActorRef]]
        val allActors = activeActors.map( _.path  ).mkString(",")
        info(s" Active actors are ${allActors}")

        /// Get a set of futures for every actor, and ask their status
        val listOfRequests: Set[Future[StatusResponse]] = activeActors.map(ask(_, WhatsYourStatus).mapTo[StatusResponse])
        val futureList = Future.sequence(listOfRequests)
        val fMap = futureList.map(_.map(_.goalStatus))

        val resultSet = Await.result(fMap, 120 seconds)
        
        //// Not quite what we want because we only want the ones for a particular track ...
        info(s" ResultSet size is ${resultSet.size} Active Actors = ${activeActors.size} ")
        /**
        while( resultSet.size < activeActors.size ) {
           info(" Result Set size is " + resultSet.size)
           resultSet = Await.result(fMap, timeout.duration)
        }
        * 
        */
        resultSet.foreach( statResp => {
            info(s" Status = ${statResp.goalName}  :: ${statResp.witness}   :: ${statResp.state} :: result = ${statResp.execResult} ")
        })
        
        
        if( resultSet.size != activeActors.size) {
           warn( "Did not receive messages from all actors ")
           val missingActors = activeActors.filter(  actor => { ! resultSet.exists( gs => { actor.path.toString.contains( gs.goalName ) }  )  } )
           resultSet ++ missingActors.map( actor => {  GoalStatus(TrackDescriptor("MissingInAction"), actor.path.name , Witness())  } )
        } else { 
           resultSet
        }
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
object ProofEngine  {

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