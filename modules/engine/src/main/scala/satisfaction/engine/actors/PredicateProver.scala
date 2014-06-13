package com.klout
package satisfaction
package engine
package actors

import com.klout.satisfaction.engine.actors.GoalStatus._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.Future
import scala.collection._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import akka.util.Timeout
import org.joda.time.DateTime
import satisfaction.track.TrackHistory


/**
 *  Actor who's responsibility is to satisfy a goal
 *
 *
 *  XXX Handle case to see if fully satisfied ( dependencies are satisfied)
 *  XXX  and cases where we want to force completion
 *
 */
class PredicateProver(val track : Track, val goal: Goal, val witness: Witness, val proverFactory: ActorRef) extends Actor with ActorLogging {

    val dependencies: mutable.Map[String, ActorRef] = scala.collection.mutable.Map[String, ActorRef]()
    var jobRunner: ActorRef = null
    val status: GoalStatus = new GoalStatus(track.descriptor, goal.name, witness)
    var listenerList: Set[ActorRef] = mutable.Set[ActorRef]()
    implicit val ec: ExecutionContext = ExecutionContext.global /// ???
    implicit val timeout = Timeout(5 minutes)
    
    var trackHistory : TrackHistory = null

    def receive = {

        case Satisfy =>
          try { 
            log.info(s" PredicateProver ${track.descriptor.trackName}::${goal.name} received Satisfy message witness is $witness ")
            log.info(s" Adding $sender to listener list")
            listenerList += sender
            if (goal.evidence != null &&
                goal.evidence.size != 0 &&
                goal.evidence.forall(e => e.exists(witness))) {
                log.info(" Check Already satisfied ?? ")
                status.state = GoalState.AlreadySatisfied
                status.timeFinished = DateTime.now
                ///sender ! GoalSuccess(status)
                publishSuccess
            } else {
                if (status.state != GoalState.Unstarted) {
                  //// XXX Test dependency on running job ...
                    sender ! InvalidRequest(status, "Job has already been started")
                } else {
                    /// Go through our dependencies, and ask them to
                    /// satify
                    if (dependencies.size > 0) {
                        dependencies.foreach {
                            case (pred, actor) =>
                                actor ! Satisfy
                        }
                        status.state = GoalState.WaitingOnDependencies
                    } else {
                        runLocalJob()
                    }
                }
            }
          } catch {
            case unexpected : Throwable =>
              unexpected.printStackTrace
              log.error( "Unexpected exception while attempting to satisfy Goal ", unexpected) 
              
              status.errorMessage = unexpected.getMessage
              ///sender ! GoalFailure( status)
              publishFailure
          }

        case WhatsYourStatus =>
            //// Do a blocking call to just return  

            ///val currentStatus = new GoalStatus(goal, witness)
            ///currentStatus.state = status.state

            /// Go through and ask all our 
            val futureSet = dependencies.map {
                case (pred, actorRef) =>
                    (actorRef ask WhatsYourStatus).mapTo[StatusResponse]
            }
            Future.sequence(futureSet).map { statusList =>
                statusList.foreach { resp =>
                    status.addChildStatus(resp.goalStatus)
                }
            }
            sender ! StatusResponse(status)
        case RestartJob =>
          log.info(s" Received RestartJob Message ; Current state is ${status.state} " )
          status.state match {
            case GoalState.Failed |
                 GoalState.Aborted =>
              /// Restart our job Runner
               runLocalJob()
            case GoalState.DependencyFailed =>
                  dependencies.foreach {
                    case (pred, actor) =>
                              log.info(s"Checking actor $pred for Job Restart")
                              //// Sequentially ask the dependencies what they're status is 
                              val checkStatusF : Future[StatusResponse] = (actor ? WhatsYourStatus).mapTo[StatusResponse]
                              val checkStatus = Await.result(checkStatusF, Duration(30, SECONDS))
                              checkStatus match {
                                case GoalState.Failed =>
                                  log.info(s"Actor $pred has job failed; sending restart... ")
                                  actor ! RestartJob
                                case GoalState.DependencyFailed =>
                                  log.info(s"Actor $pred has dependency job failed; sending restart...")
                                  actor ! RestartJob
                                case GoalState.Aborted =>
                                  log.info(s"Actor $pred has been aborted; sending restart...")
                                  actor ! RestartJob
                                case _ =>
                                  /// don't restart if job hasn't failed 
                              }
                    }
                status.state = GoalState.WaitingOnDependencies
            case _ =>
              log.error( s"Invalid Request ; Job in state ${status.state} can not be restarted." )
              sender ! InvalidRequest(status,"Job needs to have failed in order to be restarted")
          }
          /// Make Abort fire and forget for now 
        case Abort(killChildren) =>
          log.info(" Received ABORT Message; State is " + status.state)
          status.state match {
            case GoalState.Unstarted |
                 GoalState.AlreadySatisfied |
                 GoalState.Success |
            	 GoalState.Failed =>

              ///sender ! GoalSuccess( status)
            	  log.warning(s" Received Abort message, but state is ${status.state} ; Ignoring ." )
            case GoalState.Running =>
               /// If our job is running ... kill it 
              //// Check to see if abort was able to succeed ...
              log.info(" Received Abort message while Job is running; Killing Job ")
               status.state = GoalState.Aborted
               jobRunner ! Abort
               /**
               val abortResultF = jobRunner ? Abort
               val abortResult = Await.result( abortResultF, Duration( 30, SECONDS))
               abortResult match {
                 case JobRunSuccess(abortSuccess) => {
                    /// XXX Should we have abort fail ???
                   sender ! GoalSuccess( status)
                 }
                 case JobRunFailed(abortFail) => {
                   sender ! GoalFailure( status)
                 }
               }
               * 
               */
            //// check the status after attempting to abort the job
            case GoalState.DependencyFailed  |
            	 GoalState.WaitingOnDependencies =>
            	   log.info("Received Abort while DependenciesFailed, or WaitingOnDependencies ")
            	   if(killChildren) {
            	     log.info("Killing all my children.")
                     dependencies.foreach {
                       case (pred, actor) =>
                         actor ! Abort
                     }
            	   }
              //// XXX TODO wait for all children's results
              ////sender ! GoalSuccess( status)
          }

        /// Messages which can be sent from children
        case GoalFailure(failedStatus) =>
            //// 
            log.info(s" ${goal.name} Received Goal FAILURE ${failedStatus.state} from goal ${failedStatus.goalName}   ")
            status.addChildStatus(failedStatus)
            status.state = GoalState.DependencyFailed
            publishFailure
        //// Add a flag to see if we want to 
        //// abort sibling jobs which may be running 
        case GoalSuccess(depStatus) =>
            log.info(s" ${goal.name} Received Goal Success ${depStatus.state} from goal ${depStatus.goalName}   ")
            if (depStatus != null)
                status.addChildStatus(depStatus)
            //// Determine if all jobs completed
            log.info( s" Received Deps = ${status.dependencyStatus.size} :: num Deps = ${dependencies.size} ")
            if (status.dependencyStatus.size == dependencies.size) {
               if(status.canProceed) {
                  runLocalJob()
               } else {
                 /// XXX One of our children failed 
                 log.info(s" Nope --- its not OK to continue")
               }
            }
        case JobRunSuccess(result) =>
            log.info(s" ${goal.name} Received Goal Satisfied from ${result.executionName} , send to our parent  ")
            status.state = GoalState.Success
            status.execResult  = result
            status.timeFinished = DateTime.now
            publishSuccess 
        case JobRunFailed(result) =>
            log.info(s" ${goal.name} Received Goal Failed from ${result.executionName} , send to our parent  ")
            if( status.state != GoalState.Aborted)
               status.state = GoalState.Failed
            status.execResult = result
            status.timeFinished = DateTime.now
            publishFailure

        case InvalidRequest =>
          
        case unexpected : Any => {
          log.error(" Received Unexpected message " + unexpected)
          log.warning(" Received Unexpected message " + unexpected)
          log.info(" Received Unexpected message " + unexpected)
        }
    }

    def publishSuccess = {
        ////proverFactory ! Success(status)

        ///val f = proverFactory ? GetListeners(goal, witness)
        ///val response = Await.result(f, Duration(30, SECONDS))
        ///response.asInstanceOf[Set[ActorRef]].foreach { lref =>
        ///lref ! Success(status)
        ///}
        listenerList.foreach{ actor: ActorRef =>
            log.info(s" Sending GoalSuccess to $actor ")
            actor ! GoalSuccess(status)
        }
        
        proverFactory ! GoalSuccess( status)
    }
    def publishFailure = {
        listenerList.foreach{ actor: ActorRef =>
            actor ! GoalFailure(status)
        }
        proverFactory ! GoalFailure( status)
    }

    def runLocalJob() {
        if (status.state != GoalState.Running) {
            status.state = GoalState.Running
            goal.satisfier match {
                case Some(satisfier) =>
                    if( this.jobRunner == null) {
                       val jobRunActor = Props(new JobRunner(satisfier, track ,goal, witness, getWitness))
                       this.jobRunner = context.system.actorOf((jobRunActor), "Satisfier_" + ProofEngine.getActorName(goal, witness))
                    }
                    jobRunner ! Satisfy
                case None =>
                    //// XXX Refactor names 
                  /**
                    val jobRunActor = Props(new JobRunner(new WaitingSatisfier( immutable.Set(goal.evidence.toSeq: _*)),
                         track ,goal, witness, getWitness))
                    this.jobRunner = context.system.actorOf((jobRunActor), "Satisfier_" + ProofEngine.getActorName(goal, witness))
                    jobRunner ! Satisfy
                    * 
                    */
                    if( this.jobRunner == null) {
                       val jobRunActor = Props(new DefaultGoalSatisfier(
                           immutable.Set(goal.evidence.toSeq: _*), witness))
                       this.jobRunner = context.system.actorOf(jobRunActor)
                    }
                    jobRunner ! Satisfy
            }
        }
    }

    def getWitness: Witness = {
        witness
    }

    override def preStart() = {

        //// Create actors for all the sub actors, recursively
        if (goal.dependencies != null) {
            goal.dependencies.foreach{
                case (wmap: (Witness => Witness), subGoal: Goal) =>
                    //// Generate different witness, based on the mapping function
                    val newWitness = wmap(witness)
                    val depPredicate = subGoal.getPredicateString(newWitness)
                    val depProverRef = ProverFactory.getProver(proverFactory, track, subGoal, newWitness)
                    dependencies += (depPredicate -> depProverRef)
            }
        }

    }

}