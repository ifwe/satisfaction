package com.klout
package satisfaction.track

/*
Copyright 2012 Yann Ramin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/** 
 *  XXX FIXME
 *  Modified to handle Non-CronSchedules 
 *  
 */


// YY look up original source for reference
// purpose: schedule by frequency (ex// every 8 hours).

import akka.actor.{Cancellable, ActorRef, Actor}
import akka.event.Logging
import org.quartz.impl.StdSchedulerFactory
import java.util.Properties
import org.quartz._
import utils.Key
import org.joda.time.Period
import org.joda.time.DateTime
import org.joda.time.PeriodType
import org.joda.time.Seconds
import org.quartz.listeners.JobListenerSupport
import org.joda.time.Duration
import org.joda.time.DateMidnight
import org.joda.time.DurationFieldType
import akka.actor.ActorLogging


/**
 * Message to add a cron scheduler. Send this to the QuartzActor
 * @param to The ActorRef describing the desination actor
 * @param cron A string Cron expression
 * @param message Any message
 * @param reply Whether to give a reply to this message indicating success or failure (optional)
 */

case class AddCronSchedule(to: ActorRef, cron: String, message: Any, reply: Boolean = false, spigot: Spigot = OpenSpigot)

case class AddOneTimeSchedule(to: ActorRef,  offsetTime : Duration, message: Any, reply: Boolean = false, spigot: Spigot = OpenSpigot)

case class AddPeriodSchedule(to: ActorRef, period: Period, offsetTime :  Option[org.joda.time.ReadablePartial], message: Any, reply: Boolean = false, spigot: Spigot = OpenSpigot)

sealed trait AddScheduleResult

/**
 * Indicates success for a scheduler add action.
 * @param cancel The cancellable allows the job to be removed later. Can be invoked directly -
 *               canceling will send an internal RemoveJob message
 */
case class AddScheduleSuccess(startTime: DateTime, cancel: Cancellable) extends AddScheduleResult

/**
 * Indicates the job couldn't be added. Usually due to a bad cron expression.
 * @param reason The reason
 */
case class AddScheduleFailure(reason: Throwable) extends AddScheduleResult

/**
 * Remove a job based upon the Cancellable returned from a success call.
 * @param cancel
 */
case class RemoveJob(cancel: Cancellable)


/**
 * Internal class to make Quartz work.
 * This should be in QuartzActor, but for some reason Quartz
 * ends up with a construction error when it is.
 */
private class QuartzIsNotScalaExecutor() extends Job {
	def execute(ctx: JobExecutionContext) {
		val jdm = ctx.getJobDetail.getJobDataMap() // Really?
		val spigot = jdm.get("spigot").asInstanceOf[Spigot]
		if (spigot.open) {
			val msg = jdm.get("message")
			val actor = jdm.get("actor").asInstanceOf[ActorRef]
			actor ! msg
		}
	}
}



trait Spigot {
	def open: Boolean
}

object OpenSpigot extends Spigot {
  val open = true
}


/**
 * The base quartz scheduling actor. Handles a single quartz scheduler
 * and processes Add and Remove messages.
 */
class QuartzActor extends Actor with ActorLogging { // receives msg from TrackScheduler

	// Create a sane default quartz scheduler
	private[this] val props = new Properties()
	props.setProperty("org.quartz.scheduler.instanceName", context.self.path.name)
	props.setProperty("org.quartz.threadPool.threadCount", "1")
	props.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore")
	props.setProperty("org.quartz.scheduler.skipUpdateCheck", "true")	// Whoever thought this was smart shall be shot

	val scheduler = new StdSchedulerFactory(props).getScheduler


	/**
	 * Cancellable to later kill the job. Yes this is mutable, I'm sorry.
	 * @param job
	 */
	class CancelSchedule(val job: JobKey, val trig: TriggerKey) extends Cancellable {
		var cancelled = false

		def isCancelled: Boolean = cancelled

		def cancel() = {
			context.self ! RemoveJob(this)
			cancelled = true
			true
		}

	}

	override def preStart() {
		scheduler.start()
		log.info("Scheduler started")
	}

	override def postStop() {
		scheduler.shutdown()
	}
	
	def scheduleJob(to:ActorRef, schedBuilder:Option[org.quartz.ScheduleBuilder[_ <: Trigger]], message:Any,reply:Boolean,spigot:Spigot, offsetTime: Option[Either[DateTime,Duration]] = None) = {
			// Try to derive a unique name for this job
			// Using hashcode is odd, suggestions for something better?
			//val jobkey = new JobKey("%X".format((to.toString() + message.toString + cron + "job").hashCode))
			val jobkey = new JobKey("%X".format((to.toString() + message.toString + "job").hashCode))
			// Perhaps just a string is better :)
			///val trigkey = new TriggerKey(to.toString() + message.toString + cron + "trigger")
			
			val trigkey = new TriggerKey(to.toString() + message.toString +  "trigger")
			// We use JobDataMaps to pass data to the newly created job runner class
			val jd = org.quartz.JobBuilder.newJob(classOf[QuartzIsNotScalaExecutor])
			val jdm = new JobDataMap()
			jdm.put("spigot", spigot)
			jdm.put("message", message)
			jdm.put("actor", to)
			jdm.put("jobname", jobkey.getName())
			val job = jd.usingJobData(jdm).withIdentity(jobkey).build()
			
			try {
			    val tb = org.quartz.TriggerBuilder.newTrigger().withIdentity(trigkey).forJob(job)
			    val triggerBuilder : TriggerBuilder[_ <:Trigger] = if(schedBuilder.isDefined) { tb.withSchedule( schedBuilder.get)} else { tb }

			    val schedDate = offsetTime match {
			      case None => 
			        log.info(s" We don't have an offset; starting now at ${DateTime.now} ")
			       
			       scheduler.scheduleJob( job, triggerBuilder.startNow.build)
			      case Some(eitherOr) => {
                    eitherOr match {			        
                      case Left(startTime) => {
                         log.info(s" Scheduling to start at $startTime ")
			             scheduler.scheduleJob( job, triggerBuilder.startAt(startTime.toDate).build)

                      }
                      case Right(offsetDuration) => {
			       
                       log.info(s" Offset Duration specified ; Scheduling in $offsetDuration ") 
			           val later : DateTime = DateTime.now.plus( offsetDuration)

			           scheduler.scheduleJob( job, triggerBuilder.startAt(later.toDate).build)
                      }
                    }
			      }
			    }
			   	if (reply) // success case
					context.sender ! AddScheduleSuccess(new DateTime(schedDate),new CancelSchedule(jobkey, trigkey))

			} catch { // Quartz will drop a throwable if you give it an invalid cron expression - pass that info on
				case e: Throwable =>
					log.error("Quartz failed to add a task: ", e)
					if (reply)
						context.sender ! AddScheduleFailure(e)
			}
		// I'm relatively unhappy with the two message replies, but it works
	}
	
	def builderForPeriod( period : Period ) : ScheduleBuilder[ _ <: Trigger] = {
		/*
	   * notes: The standard ISO format - PyYmMwWdDThHmMsS; Substitute for lower-case; Granularity = S
	   * YY- 
	   * current limitations: cannot schedule anything that has bad precision ex// 1Month can be 30 or 31 days :(
	   */	
			val seconds=Seconds.standardSecondsIn(period)

			  CalendarIntervalScheduleBuilder.calendarIntervalSchedule
			    .withIntervalInSeconds(seconds.getSeconds)
			  
	}
	
	/// XXX Move to util class
	def dateRoundedToPeriod( dt: DateTime, per : Period ) : DateTime = {
	  log.info( s" DATE ROUNDED $dt to $per  " + dt.getHourOfDay())
	  var dtRound = dt.withSecondOfMinute(0)
	  val ftArr = per.getFieldTypes()
	  /// XXX Not quite right ...
	  /// For now, just support hours and days
	  var hasMin : Boolean = false;
	  var hasHour : Boolean = false;
	  var hasSec : Boolean = false
	  for( i <- (0 until ftArr.length - 1) ) {
	    val ft : DurationFieldType = ftArr(i)
	    val numFt = per.get(ft)
	    println(" FieldType " + numFt + " :: " + ft)
	    log.info(" FieldType " + numFt + " :: " + ft)

        ////
	    def roundFunc( dt: DateTime, idx: Int, f:(DateTime) => Int, g : (DateTime,Int) => (DateTime) ) : DateTime = {
	      val rounded = (f(dt)/idx).toInt * idx
	      log.info( idx  + "  Rounding " + f(dt) + " to " + rounded)
	      g(dt,rounded)
	    }
	    
	     if( ft ==  DurationFieldType.days() && numFt != 0 ) {
	        dtRound = roundFunc( dtRound, numFt, _.getDayOfMonth, _.withDayOfMonth(_) )
	     }
	     if( ft ==  DurationFieldType.hours() && numFt != 0 ) {
	         log.info( " HOURS hour of day = " + dtRound.getHourOfDay() )
	        dtRound = roundFunc( dtRound, numFt, _.getHourOfDay, _.withHourOfDay(_) )
	        hasHour = true
	     }
	     if( ft ==  DurationFieldType.minutes()  && numFt != 0) {
	        dtRound = roundFunc( dtRound, numFt, _.getMinuteOfHour, _.withMinuteOfHour(_) )
	        hasMin = true
	     }
	  }
	  if( !hasMin) dtRound = dtRound.withMinuteOfHour(0)
	  if( !hasHour && !hasMin) dtRound = dtRound.withHourOfDay(0)
	    
	  dtRound
	}
	
	
	def replyError( doReply : Boolean)( f: => Unit) = {
	  try {
	    f
	  } catch {
	    case unexpected : Throwable => {
	      log.warning("Unexpected error while scheduling job :: " + unexpected.getMessage, unexpected)
	      unexpected.printStackTrace(System.out)
		  if (doReply)
				context.sender ! AddScheduleFailure(unexpected)
	    } 
	  }
	}
	
	
	// Largely imperative glue code to make quartz work :)
	def receive = { // YY ? received here
		case RemoveJob(cancel) => cancel match {
			case cs: CancelSchedule => 

			  scheduler.deleteJob(cs.job);cs.cancelled = true
			case _ => log.error("Incorrect cancelable sent")
		}
		case AddCronSchedule(to, cron, message, reply, spigot) => 
		  replyError(reply) {
	        log.info( "received schedule CronJob Message")
		    val schedBuilder : ScheduleBuilder[_ <: Trigger] = org.quartz.CronScheduleBuilder.cronSchedule(cron)
		    scheduleJob(to,Some(schedBuilder),message,reply,spigot)
		  }
	
		case AddPeriodSchedule(to, period, offsetTime, message, reply, spigot) =>
		  replyError(reply) {
	        log.info( "received schedule Period Job Message")
		    val schedBuilder : ScheduleBuilder[_ <: Trigger] = builderForPeriod(period)
		    /// find offset time
            val nextTime = DateTime.now.plus( period)
		    val offsetDuration : Option[Either[DateTime,Duration]] = offsetTime match {
		       /// XXX expose complex logic so that we can see when the next time
		       /// we expect it to run ..
		      case Some(partial) => {
		         val rounded = dateRoundedToPeriod(nextTime, period)
		         Some(Left(partial.toDateTime(rounded)))
		      }
		      case _ =>  {
		         /// No Partial time ,, use roundd time
		         val rounded = dateRoundedToPeriod(nextTime, period)
		         log.info( "Perioid is  " + period)
		         log.info( "No Partitial time   Rounded date is  " + rounded)
		         Some( Left(rounded))
		      }
		    }
	        scheduleJob(to,Some(schedBuilder),message,reply,spigot, offsetDuration) 
		  }
		case AddOneTimeSchedule(to, offsetTime,  message, reply, spigot) =>
		  replyError(reply) {
	        log.info( "received schedule OneTime1G Job Message")
			scheduleJob(to, None, message, reply, spigot, Some(Right(offsetTime)))
		  }
			
		case _ => log.warning("QuartzActor::receive unreconizable message")
	}
}

