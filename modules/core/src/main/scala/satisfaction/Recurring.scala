package com.klout
package satisfaction

import org.joda.time.Period
import org.joda.time.ReadablePartial
import org.joda.time.format.ISOPeriodFormat
import org.joda.time.format.PeriodFormatter
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.JobListener
import org.joda.time.Duration

/**
 *   A Schedulable is a trait which can be attached to Trait
 *   to signify that a TrackScheduler is capable of scheduling it
 */
sealed trait Schedulable {
   def scheduleString : String 
}


/**
 *  To schedule a Track , give it the "Recurring" trait,
 *   and define a frequency and a timeOffset
 */
abstract trait Recurring  extends Schedulable {
  
  
    /**
     *   Define a Joda Period which defines the frequency
     *    that this track will be attempted to run
     *   e.g Period.days(1) 
     */
     def frequency : Period // frequency = p 1h look up ISOPeriod format (look at joda library for formatting) 
     

     /**
      *   Define an offset from the standard period interval
      *   when this track should be started.
      *   
      *   e.g. new LocalTime( 3, 15 ) for daily frequencies
      *     to run 3 hours and fifteen minutes after midnight (offset from base hour)
      */
     def timeOffset : Option[ReadablePartial] = None 
     
     
     def scheduleString =  { ISOPeriodFormat.standard.print( frequency ) }
}

/**
 *  Define specific traits for hourly and daily frequency
 */
trait Hourly extends Recurring {
   override def frequency = Temporal.hourlyPeriod
}

/**
 * 
 */
trait Daily extends Recurring {
   override def frequency = Temporal.dailyPeriod
}

/**
 *  Constantly  occurs all the time
 */
trait Constantly extends Schedulable {
    def scheduleString = "Constantly"

    /**
     *  Time to wait between job runs 
     *    before starting up again
     *    
     *  By default, wait one minute  
     */
    def delayInterval : Duration = Duration.standardSeconds(30)
    
    /**
     * If job fails, then job won't be 
     *  launched again, unless retryOnFailure
     *   is set to true
     */
    def retryOnFailure : Boolean = true
}

object Recurring {
  
     /**
      *  Parse a string to get the period
      */

     def period(periodStr : String) : Period = {
        ISOPeriodFormat.standard.parsePeriod(periodStr) 
    	//Period.parse(periodStr)

     }
}

/**
 *  If one prefers to specify a cron string for the Track,
 *  One can schedule the job with
 */
abstract trait Cronable extends Schedulable  {
 
    def cronString : String
  
    def scheduleString  : String = { cronString }
}