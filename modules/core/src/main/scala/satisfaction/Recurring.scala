package com.klout
package satisfaction

import org.joda.time.LocalTime
import org.joda.time.Period
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.ISOPeriodFormat
import org.joda.time.DateTime
import org.joda.time.Partial
import org.joda.time.ReadablePartial


sealed trait Schedulable {
   def scheduleString : String 
}


/**
 *  To schedule a Track , give it the "Recurring" trait,
 *   and define a frequency and a timeOffset
 */
trait Recurring  extends Schedulable {
  
  
    /**
     *   Define a Joda Period which defines the frequency
     *    that this track will be attempted to run
     *   e.g Period.days(1) 
     */
     def frequency : Period 
     

     /**
      *   Define an offset from the standard period interval
      *   when this track should be started.
      *   
      *   e.g. new LocalTime( 3, 15 ) for daily frequencies
      *     to run 3 hours and fifteen minutes after midnight
      */
     def timeOffset : Option[ReadablePartial] = None

}

object Recurring {
  
     /**
      *  Parse a string to get the period
      */
     def period(periodStr : String) : Period = {
         ISOPeriodFormat.standard.parsePeriod(periodStr) 
     }
  
}

/**
 *  If one prefers to specify a cron string for the Track,
 *  One can schedule the job with
 */
trait Cronable extends Schedulable {
 
    def cronString : String
  
}
  