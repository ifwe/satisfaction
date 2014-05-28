
package com.klout
package satisfaction
package track

import org.joda.time.LocalTime
import org.joda.time.Period
import engine.actors.ProofEngine
////import us.theatr.akka.quartz._
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.pattern.ask
import scala.concurrent.duration._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import fs.Path
import fs.FileSystem
import scala.concurrent.Await
import akka.util.Timeout
import akka.actor.Cancellable

/**
 *  Scheduler for different Tracks
 *  
 */
case class TrackScheduler( val proofEngine : ProofEngine ) {
   var trackFactory :  TrackFactory = null
   
   private lazy val quartzActor = ProofEngine.akkaSystem.actorOf(Props[QuartzActor])
   private lazy val startGoalActor = ProofEngine.akkaSystem.actorOf(Props[StartGoalActor])
   implicit val timeout = Timeout(24 hours)
  
   private val scheduleMap : collection.mutable.Map[TrackDescriptor,Tuple2[String,Cancellable]] = new collection.mutable.HashMap[TrackDescriptor,Tuple2[String,Cancellable]]
   
   
   /**
    *  Schedule a Track to be started according to a particular TrackSchedule
    *  
    *  returns true if the track was successfully scheduled
    */
   def scheduleTrack( track : Track ) : Boolean = {
     val trackDesc = track.descriptor
     val mess = new StartGoalMessage( track.descriptor)
     var schedString : String  = null
     val schedMess : Option[Any] =  {
        track match { // track can have two traits
          case  cronable :  Cronable =>
             schedString = cronable.scheduleString
             Some(AddCronSchedule( startGoalActor,  cronable.cronString, mess, true))
          case recurring : Recurring => // in core
             schedString = recurring.scheduleString 
             Some(AddPeriodSchedule( startGoalActor, recurring.frequency, DateTime.now , mess, true))
          case _  => None
        }
     }
     if(schedMess.isDefined) 
       sendScheduleMessage( trackDesc, schedMess.get, schedString )
     else 
       false
   }

   
   private def sendScheduleMessage( trackDesc : TrackDescriptor, schedMess : Any, schedString : String) : Boolean = {
         val addResultF =  quartzActor ? schedMess  //future
         val resultMess = Await.result( addResultF, 30 seconds )
         resultMess match { //able to schedule
            case yeah : AddScheduleSuccess => // these responses are from QuartzActor::scheduleJob
              scheduleMap.put( trackDesc, Tuple2(schedString ,yeah.cancel ))
              println(" Successfully scheduled job " + trackDesc.trackName)
              true
           case boo : AddScheduleFailure =>
              /// XXX better logging 
       	     println(" Problem trying to schedule cron " + boo.reason)
       	     boo.reason.printStackTrace()
       	     false
     }
   }
   
   
   /*
    *  Stop a scheduled Track 
    */
   def unscheduleTrack( trackDesc :TrackDescriptor ) = {
     val tup2 = scheduleMap.remove( trackDesc).get
     
     quartzActor ! tup2._2
   }
   
   
   /**
    *  List out all the current Tracks which have been scheduled,
    *    
    */
   def getScheduledTracks : collection.Set[Tuple2[TrackDescriptor,String]] = {
       scheduleMap.keySet.map( td => { Tuple2(td,scheduleMap.get(td).get._1) } )
   }
   


    
case class StartGoalMessage( val trackDesc : TrackDescriptor )
   
   class StartGoalActor( trackFactory : TrackFactory, proofEngine : ProofEngine ) extends Actor with ActorLogging {
       def receive = {
         case mess : StartGoalMessage =>
           log.info(" Starting Track " + mess.trackDesc +  " TrackFactory = " + TrackFactory)
           val trckOpt =  trackFactory.getTrack( mess.trackDesc )
           trckOpt match {
             case Some(trck) =>
        	   val witness = generateWitness(trck, DateTime.now)
        	   
        	   trck.topLevelGoals.foreach( goal => { 
        	      log.info(s" Satisfying Goal $goal.name with witness $witness ")
        	      goal.variables.foreach( v => println( s"  Goal $goal.name has variable " + v))
                  proofEngine.satisfyGoal( goal, witness)
              } )
             case None =>
              println(" Track " + mess.trackDesc.trackName + " not found ")
           }
         
       } 
   }
   
   def generateWitness( track : Track, nowDt : DateTime ) : Witness = {
     var subst = Witness()
     track.getWitnessVariables.foreach( { v : Variable[_] =>
     	if(isTemporalVariable(v)) {
     		val temporal = getValueForTemporal( v, nowDt)
     		subst = subst + VariableAssignment( v.name , temporal)
     		println(s" Adding Temporal value $temporal for temporal variable $v.name ")
     	} else {
     	  /// XXX Fixme  ???? Allow the substitution to be partially specified
     	  println(s" Getting non temporal variable $v.name from track properties ")
     	  val varValMatch = track.trackProperties.raw.get( v.name)
     	  
     	  varValMatch match {
     	    case Some(varVal) =>
   		      subst = subst + VariableAssignment( v.name , varVal)
     	    case None =>
     	      println(" No variable found with " + v.name)
     	  }
     	}
     })
     println(" Temporal witness is " + subst)
     subst
   }
     /**
    *   Add TemporalVariable trait, 
    *     and push to common...
    *    for now, just check if "dt"
    *    
    */
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


