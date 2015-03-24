package satisfaction
package engine
package actors

import GoalStatus._
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
import track.TrackHistory
import akka.actor.ActorPath
import javax.naming.InvalidNameException
import akka.actor.InvalidActorNameException
import java.util.concurrent.TimeUnit


/**
 *  Actor who's responsibility is to satisfy a goal
 *
 *
 * 
 *
 */
class PredicateProver(val track : Track, val goal: Goal, val witness: Witness, val proverFactory: ActorRef) extends Actor with satisfaction.Logging {
    private val variableCheck = {
      goal.variables.foreach( v => {
         if( ! witness.contains(v))  {
           error(" Goal ${track.descriptor.name}::${goal.name} requires more variables then specifed in Witness !   $v not in witness $witness ")
           throw new RuntimeException (" Goal ${track.descriptor.name}::${goal.name} requires more variables then specifed in Witness !   $v not in witness $witness ")
         }
      } )
    }

    ///private val dependencies: mutable.Map[String, ActorRef] = scala.collection.mutable.Map[String, ActorRef]()
    private  lazy val  _dependencies: Map[(Goal,Witness), ActorRef] =  initDependencies
    

    private var jobRunner: ActorRef = null

    val status: GoalStatus = new GoalStatus(track.descriptor, goal.name, witness)

    private val _listenerList: mutable.Set[ActorRef] = mutable.Set[ActorRef]()
    private val _evidenceCheckers : mutable.Map[String,ActorRef] = mutable.Map[String,ActorRef]()

    def listenerList : immutable.Set[ActorRef] =  { _listenerList.toSet }
    
    implicit val ec: ExecutionContext = ExecutionContext.global /// ???
    implicit val timeout = Timeout(5 minutes) ///XXX from Config
   
    private var runID: String  = ""
    private var parentRunID : String = ""
    private var forceSatisfy : Boolean = false
    
    def failureCheck( f : => Unit ) : Unit = {
       try {
           f
       } catch {
            case unexpected : Throwable =>
              unexpected.printStackTrace
              error( s"Unexpected exception while attempting to satisfy Goal ${goal.name} ${witness}", unexpected) 
              status.markUnexpected( unexpected)
              publishFailure
       }
    }


    def receive = {

        case Satisfy(runID,parentRunID,forceSatisfy) =>
          failureCheck { 
            this.runID = runID
            this.parentRunID = parentRunID
            this.forceSatisfy = forceSatisfy
            info(s" PredicateProver ${track.descriptor.trackName}::${goal.name} $witness received Satisfy message witness is $witness with runID =${runID} parentRunID=${parentRunID} forceSatisfy=${forceSatisfy} ")
            info(s" Adding $sender to listener list")
            addListener( sender )
            if (goal.hasEvidence &&
                forceSatisfy == false ) {
                 //// Create evidence In  
              
                goal.evidenceForWitness(witness).foreach(e => {
                    sendCheckEvidence(e)   
                } )
            } else {
              if(forceSatisfy) {
                 warn(s" Forcing Satisfy for  ${goal.name} ${witness} ; forcing Satisfy ")
              }
              satisfy(runID,parentRunID,forceSatisfy)
            }
          }
    case EvidenceCheckResult(id: String, w: Witness, isAlreadySatisfied: Boolean) =>
      failureCheck {
        ////First thing, just remove from evidence
        info(s"Goal ${goal.name} $witness received EvidenceCheckResult $id AlreadySatisfied = $isAlreadySatisfied ")
        _evidenceCheckers.remove(id) match {
          case Some(ecActor) => {
            context.system.stop(ecActor)

            if (!isAlreadySatisfied
              && status.state == GoalState.CheckingEvidence) {
              if (!forceSatisfy) {
                satisfy(this.runID, this.parentRunID, this.forceSatisfy);
              } else {
                runLocalJob()
              }
            } else if (isAlreadySatisfied
              && status.state == GoalState.CheckingEvidence
              && _evidenceCheckers.size == 0) {

              //// Already satisfied 
              info(s" Goal ${goal.name} Already satisfied for Witness $w ; releasing dependencies ")
              dependencies.foreach {
                case (predTuple, actor) => proverFactory ! ReleaseActor(predTuple._1.name, predTuple._2)
              }
              status.markTerminal(GoalState.AlreadySatisfied)
              publishSuccess
            }
          }
          case None => {
        	 warn(s" No evidence found for $id for ${goal.name} $witness ; Ignoring")
          }
        }
      }
        case WhatsYourStatus =>
          try  {
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
          } catch {
            case unexpected : Throwable => {
                status.markUnexpected(unexpected)
                sender ! StatusResponse(status)
             }
          }
        case RestartJob =>
          info(s" Received RestartJob Message ; Current state is ${status.state} " )
          status.state match {
            case GoalState.Failed |
                 GoalState.Aborted =>
              /// Restart our job Runner
               runLocalJob()
            case GoalState.DependencyFailed =>
                  dependencies.foreach {
                    case (pred, actor) =>
                              info(s"Checking actor $pred for Job Restart")
                              //// Sequentially ask the dependencies what they're status is 
                              val checkStatusF : Future[StatusResponse] = (actor ? WhatsYourStatus).mapTo[StatusResponse]
                              val checkStatus = Await.result(checkStatusF, Duration(30, SECONDS))
                              checkStatus match {
                                case GoalState.Failed =>
                                  info(s"Actor $pred has job failed; sending restart... ")
                                  actor ! RestartJob
                                case GoalState.DependencyFailed =>
                                  info(s"Actor $pred has dependency job failed; sending restart...")
                                  actor ! RestartJob
                                case GoalState.Aborted =>
                                  info(s"Actor $pred has been aborted; sending restart...")
                                  actor ! RestartJob
                                case _ =>
                                  /// don't restart if job hasn't failed 
                              }
                    }
                status.transitionState( GoalState.WaitingOnDependencies )
            case _ =>
              error( s"Invalid Request ; Job in state ${status.state} can not be restarted." )
              sender ! InvalidRequest(status,"Job needs to have failed in order to be restarted")
          }
          /// Make Abort fire and forget for now 
        case Abort(killChildren) =>
          info(" Received ABORT Message; State is " + status.state)
          status.state match {
            case GoalState.Success |
                 GoalState.AlreadySatisfied =>
           	  warn(s" Received Abort message, but state is ${status.state} ; Ignoring ." )
            case GoalState.Failed =>
               warn(s" Received Abort message after failure,  Killing self." )
               status.markTerminal(GoalState.Aborted )
               proverFactory ! ReleaseActor( goal.name, witness)
               context.system.scheduler.scheduleOnce( 10 seconds, self, new KillActor(goal.name,witness) )
            case GoalState.Running =>
               /// If our job is running ... kill it 
              //// Check to see if abort was able to succeed ...
               info(" Received Abort message while Job is running; Killing Job ")
               status.markTerminal(GoalState.Aborted )
               jobRunner ! Abort
            //// check the status after attempting to abort the job
            case GoalState.DependencyFailed  |
            	 GoalState.Unstarted |
            	 GoalState.WaitingOnDependencies =>
            	   info("Received Abort while DependenciesFailed, or WaitingOnDependencies ")
            	   if(killChildren) {
            	     info("Killing all my children.")
                     dependencies.foreach {
                       case (pred, actor) =>
                         actor ! Abort
                     }
            	   }
                proverFactory ! ReleaseActor( goal.name, witness)
                context.system.scheduler.scheduleOnce( 10 seconds, self, new KillActor(goal.name,witness) )
          }

        /// Messages which can be sent from children
        case GoalFailure(failedStatus) =>
            //// XXX 
            //// Should we try tp retry now ??? 
            //// or wait or rety agent to get back to us ???
            info(s" ${goal.name} Received Goal FAILURE ${failedStatus.state} from goal ${failedStatus.goalName}   ")
            status.addChildStatus(failedStatus)
            status.markTerminal(GoalState.DependencyFailed )
            publishFailure
        //// Add a flag to see if we want to 
        //// abort sibling jobs which may be running 
        case GoalSuccess(depStatus) =>
            info(s" ${goal.name} Received Goal Success ${depStatus.state} from goal ${depStatus.goalName}   ")
            if (depStatus != null)
                status.addChildStatus(depStatus)
            //// Determine if all jobs completed
            info( s" Received Deps = ${status.numReceivedStatuses} :: num Deps = ${dependencies.size} ")
            
            if (status.numReceivedStatuses  >= dependencies.size) {
               if(status.canProceed) {
                 info(s"Goal ${goal.name} $witness Received all dependencies .. Proceding ")
                 if(!forceSatisfy) {
                   runLocalJob()
                 } else {
                   if( goal.hasEvidence) {
                     goal.evidenceForWitness(witness).foreach(e => {
                        sendCheckEvidence(e)   
                     })
                   } else {
                     info(s"Goal ${goal.name} $witness No Evidence -- Run our local job")
                     runLocalJob()
                   }
                 } 
               } else {
                 /// XXX One of our children failed 
                 info(s" Nope --- its not OK to continue")
               }
            }
        case JobRunSuccess(result) =>
            info(s" ${goal.name} Received JobRunSuccess from ${result.executionName} , send to our parent  ")
            status.markExecution(result)
            publishSuccess 
        case JobRunFailed(result) =>
            info(s" ${goal.name} Received JobRunFailed from ${result.executionName} , send to our parent  ")
            jobRunner = null
            status.markExecution(result)
            status.markTerminal( GoalState.Failed, DateTime.now)
            publishFailure
            
        //// If I receive a Release actor message, 
        //// Tell the ProverFactory my dependencies to release
        //// Assume that we've already completed 
        case ReleaseActor =>     
            dependencies.foreach {
                case (pred, actor) =>
                     proverFactory ! ReleaseActor(pred._1.name,pred._2)
            }
        case AddListener(actorRef) => 
          _listenerList.add( actorRef)

        case InvalidRequest =>
          
        case unexpected : Any => {
          error(" Received Unexpected message " + unexpected + " of type " + unexpected.getClass )
          warn(" Received Unexpected message " + unexpected)
          info(" Received Unexpected message " + unexpected)
        }
    }
    
    def sendCheckEvidence(e: Evidence) = {
      val id = ( _evidenceCheckers.size +1 ).toString
      status.transitionState( GoalState.CheckingEvidence)
      
      val evidenceCheckerProps = Props( classOf[EvidenceChecker], e )
      val evidenceCheckerActor = context.system.actorOf((evidenceCheckerProps), "Evidence_" + id + "_" + ProofEngine.getActorName(goal, witness))
      _evidenceCheckers.put( id, evidenceCheckerActor)
      
      val checkEvidenceMethod = new Runnable {
        override def run( ) = { 
          if( status.state  == GoalState.CheckingEvidence) {
             log.info(s" Checking Evidence ${e} for ${goal.name} for witness $witness" )
             evidenceCheckerActor ! CheckEvidence(id, witness)
             try {
               /// Re-send check evidence, in case message gets lost or forgotten
               val delayDuration : FiniteDuration =  Duration.create( 60, TimeUnit.SECONDS)
               if(context != null) {
                 context.system.scheduler.scheduleOnce(delayDuration, this)
               } else {
                 log.warn(s" Context is null when attempting to reschedule check evidence ")
               }
             } catch {
               case unexpected : Throwable => {
                 log.error(s" Unexpected exception attempting to reschedule CheckEvidence ${unexpected.getMessage()} ", unexpected)
               }
             }
         }
        }
      }
      
      checkEvidenceMethod.run()
    }
    
    def satisfy(runID : String,parentRunID : String, forceSatisfy:Boolean) = {
       /// Go through our dependencies, and ask them to
      /// satisfy themselves 
      if (goal.hasDependencies ) {
           status.transitionState ( GoalState.WaitingOnDependencies)
           dependencies.foreach {
                case (pred, actor) => {
                    info(s" Sending Satisfy Message to ${pred._1} ${pred._2} ")
                    actor ! Satisfy(runID=null,parentRunID=runID,forceSatisfy)
                }
           }
      } else {
        if(forceSatisfy
            && goal.hasEvidence) {
               info(s" Goal ${goal.name} $witness has no dependencies; checking Evidence if already satisfied")
                goal.evidenceForWitness(witness).foreach(e => {
                    sendCheckEvidence(e)   
                } )
        } else {
           runLocalJob()
        }
      }
    }

    def publishSuccess = {
        listenerList.foreach{ actor: ActorRef =>
            info(s" Sending GoalSuccess to $actor ")
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
    
    def addListener( actor : ActorRef) = {
      _listenerList.add( actor)
    }

    def runLocalJob() {
        if (status.state != GoalState.Running) {
            status.transitionState( GoalState.Running )
            goal.newSatisfier(witness) match {
                case Some(satisfier) =>
                  if( jobRunner == null) {
                    try {
                       val jobRunActor = Props(new JobRunner(satisfier, track ,goal, witness, witness))
                       this.jobRunner = context.system.actorOf((jobRunActor), "Satisfier_" + ProofEngine.getActorName(goal, witness))
                       log.info(s"Actor ${this.self.path} created Actor ${jobRunner.path} ")
                    } catch {
                      case invName : Throwable => {
                         error(s" Unexpected error while creating actor  ${goal.name} $witness", invName)
                         status.transitionState( GoalState.Failed)
                         status.markUnexpected(invName)
                         publishFailure
                         return
                      }
                    }
                  } else {
                     warn(s" JobRunner already exists for goal ${goal.name} $witness")   
                  }
                  jobRunner ! Satisfy
                  //// If the satisfier reports progress,
                  ////   Grab the progressable...
                  satisfier match {
                      case progressable : Progressable =>
                        info(s" Grabbing Progress for current satisfier for $goal.name -- progress $progressable.progressCounter ")
                       this.status.setProgressCounter( progressable.progressCounter )
                      case _ =>
                        info(s" Unable to determine progress for goal $goal.name ")
                    }
                case None =>
                   if( jobRunner == null) {
                    val jobRunActor = Props(new DefaultGoalSatisfier(
                            track,goal,
                           immutable.Set(goal.evidenceForWitness(witness): _*), witness))
                    this.jobRunner = context.system.actorOf(jobRunActor)
                   }
                    jobRunner ! Satisfy
            }
        }
    }

    def dependencies : Map[(Goal,Witness),ActorRef] = {
      status.state match {
        case GoalState.AlreadySatisfied => Map.empty
        case GoalState.Unstarted => Map.empty
        case GoalState.Running => Map.empty
        case _ => {
           _dependencies
        } 
      }
    } 

    ///override def preStart() = {
    def initDependencies : Map[(Goal,Witness),ActorRef] = {
      if(goal.hasDependencies) {
        info(" Initializing Dependencies !!!")
        println(s" Initializing Dependencies ${goal.name}  $witness !!!  GOAL IS $goal ")
        val deps = goal.dependenciesForWitness(witness)
        println(s" DEPS is ${deps.size} ")
        goal.dependenciesForWitness(witness).map(  { case(wmap:(Witness =>Witness),subGoal:Goal) => {
              val newWitness = wmap( this.witness)
               info(s"  ${goal.name} ${witness} has sub-dependency ${subGoal.name} ${newWitness} !!!")
               println(s"  ${goal.name} ${witness} has sub-dependency ${subGoal.name} ${newWitness} !!!")
              val depProverRef = ProverFactory.acquireProver(proverFactory,track,subGoal,newWitness)
              ( (subGoal,newWitness) -> depProverRef)
           }
        } ).toMap
      } else { 
        Map.empty
      }
    }

}