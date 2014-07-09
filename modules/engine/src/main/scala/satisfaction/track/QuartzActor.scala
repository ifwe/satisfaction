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

trait AddScheduleResult

/**
 * Indicates success for a scheduler add action.
 * @param cancel The cancellable allows the job to be removed later. Can be invoked directly -
 *               canceling will send an internal RemoveJob message
 */
case class AddScheduleSuccess(cancel: Cancellable) extends AddScheduleResult

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
class QuartzActor extends Actor { // receives msg from TrackScheduler
	val log = Logging(context.system, this)

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
	
	def scheduleJob(to:ActorRef, schedBuilder:Option[org.quartz.ScheduleBuilder[_ <: Trigger]], message:Any,reply:Boolean,spigot:Spigot, offsetTime: Option[Duration] = None) = {
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
			    offsetTime match {
			      case None => 
			        println("we don't have an offset!!!")
			       
			       scheduler.scheduleJob( job, triggerBuilder.startNow.build)
			      case Some(offsetDuration) =>
			       println("we have an offset!!!")
			       
			       val later : DateTime = DateTime.now.plus( offsetDuration)

			       scheduler.scheduleJob( job, triggerBuilder.startAt(later.toDate).build)
			    }
			   	if (reply) // success case
					context.sender ! AddScheduleSuccess(new CancelSchedule(jobkey, trigkey))

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
	
	def dateRoundedToPeriod( dt: DateTime, per : Period ) : DateTime = {
	  var dtRound = DateTime.now.withSecondOfMinute(0)
	  val ftArr = per.getFieldTypes()
	  /// XXX Not quite right ...
	  /// For now, just support hours and days
	  var hasMin : Boolean = false;
	  var hasHour : Boolean = false;
	  for( i <- (0 until ftArr.length - 1) ) {
	    val ft : DurationFieldType = ftArr(i)

	    def roundFunc( dt: DateTime, idx: Int, f:(DateTime) => Int, g : (DateTime,Int) => (DateTime) ) : DateTime = {
	      val rounded = (f(dt)/idx).toInt * idx
	      g(dt,rounded)
	    }
	    
	     if( ft ==  DurationFieldType.days() ) {
	        dtRound = roundFunc( dtRound, per.get(ft), _.getDayOfMonth, _.withDayOfMonth(_) )
	     }
	     if( ft ==  DurationFieldType.hours() ) {
	        dtRound = roundFunc( dtRound, per.get(ft), _.getHourOfDay, _.withHourOfDay(_) )
	        hasHour = true
	     }
	     if( ft ==  DurationFieldType.minutes() ) {
	        dtRound = roundFunc( dtRound, per.get(ft), _.getMinuteOfHour, _.withMinuteOfHour(_) )
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
	      log.info("Unexepected error while scheduling job :: " + unexpected.getMessage, unexpected)
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
	        log.info( "received schedule Job Message")
		    val schedBuilder : ScheduleBuilder[_ <: Trigger] = org.quartz.CronScheduleBuilder.cronSchedule(cron)
		    scheduleJob(to,Some(schedBuilder),message,reply,spigot)
		  }
	
		case AddPeriodSchedule(to, period, offsetTime, message, reply, spigot) =>
		  replyError(reply) {
		    val schedBuilder : ScheduleBuilder[_ <: Trigger] = builderForPeriod(period)
		    /// find offset time
            val nw = DateTime.now
		    val offsetDuration : Option[Duration] = offsetTime match {
		       /// XXX expose complex logic so that we can see when the next time
		       /// we expect it to run ..
		      case Some(partial) =>
		         val nextTime = partial.toDateTime(nw)
		         if( nextTime.isAfter( nw)) {
		           Some(new Duration( nw, nextTime))
		         } else {
		            val nextPeriod = nextTime.plus(period)
		            Some(new Duration( nw, nextPeriod))
		         }
		      case _ => 
		         /// No Partial time ,, use roundd time
		         val rounded = dateRoundedToPeriod( nw, period)
		         Some( new Duration( rounded, nw))
		    }
	       scheduleJob(to,Some(schedBuilder),message,reply,spigot, offsetDuration) 
		  }
		case AddOneTimeSchedule(to, offsetTime,  message, reply, spigot) =>
		  replyError(reply) {
			scheduleJob(to, None, message, reply, spigot, Some(offsetTime))
		  }
			
		case _ => log.warning("QuartzActor::receive unreconizable message")
	}
}

