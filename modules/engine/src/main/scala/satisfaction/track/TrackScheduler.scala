package com.klout
package satisfaction
package track

import org.joda.time.LocalTime
import org.joda.time.Period
import engine.actors.ProofEngine
import us.theatr.akka.quartz._
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
case class TrackScheduler( private val proofEngine : ProofEngine ,   
      val trackFactory :  TrackFactory) {
   
   private lazy val quartzActor = ProofEngine.akkaSystem.actorOf(Props[QuartzActor])
   private lazy val startGoalActor = ProofEngine.akkaSystem.actorOf(Props[TrackScheduler.StartGoalActor])
   implicit val timeout = Timeout(24 hours)
  
   private val scheduleMap : collection.mutable.Map[TrackDescriptor,Tuple2[TrackSchedule,Cancellable]] = new collection.mutable.HashMap[TrackDescriptor,Tuple2[TrackSchedule,Cancellable]]
   
   
   
   /** Generate a witness for a specific time
    *  //// XXX 
    */
   def generateWitness( track : Track, nowDt : DateTime ) : Witness = {
     var subst = Substitution()
     track.getWitnessVariables.foreach( { v : Variable[_] =>
     	if(isTemporalVariable(v)) {
     		val temporal = getValueForTemporal( v, nowDt)
     		subst = subst + VariableAssignment( v.name , temporal)
     		subst = subst + VariableAssignment( "dateString" , temporal)
     		println(s" Adding Temporal value $temporal for temporal variable $v.name ")
     	} else {
     	  /// XXX Fixme  ???? Allow the substitution to be parially specified
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
     new Witness( subst)
   }
   
   
   /**
    *   XXX Add TemporalVariable trait, 
    *     and push to common...
    *    for now, just check if "dt"
    */
   def isTemporalVariable( variable : Variable[_] ) : Boolean = {
     variable.name.equals("dt") || variable.name.equals( "dateString")
   }
   
   def getValueForTemporal( variable : Variable[_], dt : DateTime ) : Any = {
       val YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd")

       YYYYMMDD.print( dt) 
   }
   
   
   
   /**
    *  Schedule a Track to be started according to a particular TrackSchedule
    *  
    *  returns true if the track was successfully scheduled
    */
   def scheduleTrack( trackDesc : TrackDescriptor, sched : TrackSchedule  ) : Boolean = {
     val cronPhrase = sched.getCronString
     val mess = new TrackScheduler.StartGoalMessage( trackDesc)
     val addResultF =  quartzActor ? AddCronSchedule( startGoalActor,  cronPhrase, mess, true)
     val resultMess = Await.result( addResultF, 30 seconds )
     resultMess match {
       case yeah : AddCronScheduleSuccess =>
          scheduleMap.put( trackDesc, Tuple2(sched ,yeah.cancel ))
          println(" Successfully scheduled job " + trackDesc.trackName)
          true
       case boo : AddCronScheduleFailure =>
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
   def getScheduledTracks : collection.Set[Tuple2[TrackDescriptor,TrackSchedule]] = {
       scheduleMap.keySet.map( td => { Tuple2(td,scheduleMap.get(td).get._1) } )
   }
   
}

/// Companion object
object TrackScheduler extends TrackScheduler( ProofEngine, TrackFactory) {
    
   case class StartGoalMessage( val trackDesc : TrackDescriptor )
   
   class StartGoalActor extends Actor with ActorLogging {
       def receive = {
         case mess : StartGoalMessage =>
           
           //// XXX Think about generating top-level witnesses ...
           ////  Have available in Track Properties ???
           log.info(" Starting Track " + mess.trackDesc +  " TrackFactory = " + TrackFactory)
           val trckOpt =  TrackFactory.getTrack( mess.trackDesc )
           trckOpt match {
             case Some(trck) =>
        	   val witness = generateWitness(trck, DateTime.now)
        	   
        	   trck.topLevelGoals.foreach( goal => { 
        	      log.info(s" Satisfying Goal $goal.name with witness $witness ")
        	      goal.variables.foreach( v => println( s"  Goal $goal.name has variable " + v))
                  proofEngine.satisfyGoal(trck,goal, witness)
              } )
             case None =>
              println(" Track " + mess.trackDesc.trackName + " not found ")
           }
         
       } 
   }
  
}


