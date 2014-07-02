package com.klout
package satisfaction

import org.joda.time.Period
import org.joda.time.ReadablePartial
import org.joda.time.format.ISOPeriodFormat
import org.joda.time.format.PeriodFormatter


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
     def timeOffset : Option[ReadablePartial] = None // this can get ugly. Work carefully.
     
     
     def scheduleString =  { ISOPeriodFormat.standard.print( frequency ) }
     
     def startTime = 123 // remember the initial start time. Maybe move this to Constantly only
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

trait Constantly extends Recurring {
  override def frequency = Temporal.continuousFrequency
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
abstract trait Cronable extends Schedulable {
 
    def cronString : String
  
    def scheduleString  : String = { cronString }
}
 

/**
 *	A Stoppable is a trait which can be attached to a Track
 *	If a job is running when it is scheduled to run, 
 * 	don't run it ( in case something has been delayed ).
 *  
 *  Question: when this happens, do we delete this run, or just delay it?
 */
