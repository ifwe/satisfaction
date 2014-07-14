
package com.klout
package satisfaction
package track

import scala.concurrent.Await
import scala.concurrent.duration._
import org.joda.time.DateTime
import org.joda.time.Period
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Cancellable
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import engine.actors.ProofEngine
import fs.FileSystem
import satisfaction.engine.actors.GoalStatus
import akka.dispatch.OnSuccess
import concurrent._
import ExecutionContext.Implicits.global
import satisfaction.engine.actors.GoalState
import scala.util.Try
import scala.util.Success
import scala.util.Failure


/**
 *  Scheduler for different Tracks
 *  
 */
case class TrackScheduler( val proofEngine : ProofEngine ) extends Logging  {
	var trackFactory :  TrackFactory = null
   
   private lazy val quartzActor = proofEngine.akkaSystem.actorOf(Props[QuartzActor])
   private lazy val startGoalActor = proofEngine.akkaSystem.actorOf(Props( new StartGoalActor( trackFactory, proofEngine)) )
   implicit val timeout = Timeout(24 hours)
  
   // Format trackDesc => [schedString, Cancellable, Pauseable]
   private val scheduleMap : collection.mutable.Map[TrackDescriptor,Tuple3[String,Cancellable, Boolean]] = new collection.mutable.HashMap[TrackDescriptor,Tuple3[String,Cancellable,Boolean]]
   
      class StartGoalActor( trackFactory : TrackFactory, proofEngine : ProofEngine ) extends Actor with ActorLogging {
       def receive = {
         case mess : StartGoalMessage =>
           log.info(" Starting an instance of Track " + mess.trackDesc /*+  " TrackFactory = " + trackFactory*/)
           
           val trackScheduling = scheduleMap.get(mess.trackDesc)
           val pausable = {
             trackScheduling match {
               case Some(schedInfo) if (schedInfo._3) =>
	               true
               case _ =>
                 false
             }
           }
           val trckOpt =  trackFactory.getTrack( mess.trackDesc )

           trckOpt match {
	             case Some(trck) if (pausable && isRunning(trck)) => // define "already running"
                   log.info("   this instance of me cannot run - going to discard now")
	             case Some(trck) =>
	               val witness = generateWitness(trck, DateTime.now)
		           trck.topLevelGoals.foreach( goal => { 
		        	    log.info(s" Satisfying Goal $goal.name with witness $witness ")
		                val goalFuture = proofEngine.satisfyGoal( goal, witness)

		                if( mess.continuous) {
		                   goalFuture.onSuccess {
		                      case gs : GoalStatus => {
		                         trck match {
	                               case always : Constantly => {
		                            if( gs.state == GoalState.Success ||  always.retryOnFailure ) {
	                                  log.info(" Track is constantly Recurring; restarting in " + always.delayInterval)
	                                  val schedMess = AddOneTimeSchedule(startGoalActor, always.delayInterval , mess, true)
		                              sendScheduleMessage( mess.trackDesc, schedMess, always.scheduleString , false)
		                            }
		                          }
	                               case _ => {
	                                 log.info(" Track doesn't extend Constantly, so not restarting ")
	                               }
		                        }
		                     }
		                  }
		                }
		              } )
	             case None =>
	              println(" Track " + mess.trackDesc.trackName + " not found ")
	           }
       }
   }
   
   /**
    * Check if a job is currently running
    */
   
   def isRunning(track: Track) : Boolean = {
     
     !proofEngine.getGoalsInProgress.filter(running => track.topLevelGoals.map(_.name).contains(running.goalName)).isEmpty

       //proofEngine.getGoalsInProgress.foreach(p => if (track.topLevelGoals.map(_.name).contains(p.goalName)) true)
     /*
     val witness = generateWitness(track, DateTime.now) // how do I reference the correct witness??? - in the case of temporal variable - i have no idea how to match witnesses :/ 
     
     val runningForTrack = track.topLevelGoals.filter(goal => proofEngine.getStatus(goal, witness).canChange)
     !runningForTrack.isEmpty
      */
     
   }
   
   
   /**
    *  Schedule a Track to be started according to a particular TrackSchedule
    *  
    *  returns Success if the track was successfully scheduled
    */
   def scheduleTrack(track :Track) : Try[String] = {
     scheduleTrack(track, false)
   }
   
   def scheduleTrack( track : Track , pausable : Boolean) : Try[String] = {
     val trackDesc = track.descriptor
     val schedMess : Option[Tuple2[Any,String]] =  {
        track match { // track can have two traits
          case  cronable :  Cronable =>
             val mess = new StartGoalMessage( track.descriptor, false)
             Some((AddCronSchedule( startGoalActor,  cronable.cronString, mess, true),cronable.scheduleString))
          case constant : Constantly =>
            val mess = new StartGoalMessage( track.descriptor, true)
            Some((AddOneTimeSchedule(startGoalActor, constant.delayInterval , mess, true),constant.scheduleString))
          case recurring : Recurring => // in core
             val mess = new StartGoalMessage( track.descriptor, true)
             Some((AddPeriodSchedule( startGoalActor, recurring.frequency, recurring.timeOffset , mess, true),recurring.scheduleString))
          case _  => None
        }
     }
     if(schedMess.isDefined) 
       sendScheduleMessage( trackDesc, schedMess.get._1, schedMess.get._2, pausable)
     else 
       Failure( new RuntimeException("Unable to schedule track " + track.descriptor.trackName + " ; no Schedulable trait "))
   }

   
   private def sendScheduleMessage( trackDesc : TrackDescriptor, schedMess : Any, schedString : String, pausable : Boolean) : Try[String] = {
         val addResultF =  quartzActor ? schedMess  //future
         val resultMess = Await.result( addResultF, 30 seconds )
         resultMess match { //able to schedule
            case yeah : AddScheduleSuccess => // these responses are from QuartzActor::scheduleJob
              scheduleMap.put( trackDesc, Tuple3(schedString ,yeah.cancel, pausable))
              info(" Successfully scheduled job " + trackDesc.trackName + " is it pausable? " + pausable)
              Success(" Successfully scheduled job " + trackDesc.trackName + " is it pausable? " + pausable)
           case boo : AddScheduleFailure =>
       	     info(" Problem trying to schedule cron " + boo.reason,boo.reason)
       	     boo.reason.printStackTrace()
       	     Failure( boo.reason)
     }
   }
  
   
   /*
    *  Stop a scheduled Track 
    */
   def unscheduleTrack( trackDesc :TrackDescriptor ) = {
     val tup3 = scheduleMap.remove( trackDesc).get
     info("  unscheduleTrack: going to unschedule tuple: " + tup3._1 + " " + tup3._2 + " " + tup3._3)
     quartzActor ! RemoveJob(tup3._2)
   }
   
   
   /**
    *  List out all the current Tracks which have been scheduled,
    *    
    */
   def getScheduledTracks : collection.Set[Tuple2[TrackDescriptor,String]] = {
     //YY might have to refactor this - new pausable
       scheduleMap.keySet.map( td => { 
         Tuple2(td,scheduleMap.get(td).get._1) } )
   }
   


    
   case class StartGoalMessage( val trackDesc : TrackDescriptor, val continuous : Boolean )
   
  
   
   def generateWitness( track : Track, nowDt : DateTime ) : Witness = {
     var subst = Witness()
     track.getWitnessVariables.foreach( { v : Variable[_] =>
     	if(isTemporalVariable(v)) {
     		val temporal = getValueForTemporal( v, nowDt)
     		subst = subst + VariableAssignment( v.name , temporal)
     		info(s" Adding Temporal value $temporal for temporal variable $v.name ")
     	} else {
     	  /// XXX Fixme  ???? Allow the substitution to be partially specified
     	  info(s" Getting non temporal variable $v.name from track properties ")
     	  val varValMatch = track.trackProperties.raw.get( v.name)
     	  
     	  varValMatch match {
     	    case Some(varVal) =>
   		      subst = subst + VariableAssignment( v.name , varVal)
     	    case None =>
     	      info(" No variable found with " + v.name)
     	  }
     	}
     })
     info(" Temporal witness is " + subst)
     subst
   }

   def isTemporalVariable( variable : Variable[_] ) : Boolean = {
      variable match {
        case temporal : TemporalVariable => true
        case _ => false
      }
   }
   
   def getValueForTemporal( variable : Variable[_], dt : DateTime ) : String = {
       variable match  {
         case temporal : TemporalVariable => temporal.formatted( dt)
         case _ => ""
       }
   }
   
  
}

