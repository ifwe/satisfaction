package com.klout
package satisfaction

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter


/**
 *  Associate a notion of Time for certain variables
 */
trait TemporalVariable {
    val FormatString :String
  
    def formatted( dt : DateTime) : String = {
       formatter.print(dt)
    }
   
    lazy val formatter : DateTimeFormatter = DateTimeFormat.forPattern( FormatString)
    
    object DAILY extends Variable[String]("dt", classOf[String], Some("Daily Frequency")) with TemporalVariable {
       override val FormatString = "YYYYMMDD"      
    }

    /// Alternative Daily Frequency VAr
    object DATE extends Variable[String]("date", classOf[String], Some("Alternative Daily Frequency Varr")) with TemporalVariable {
       override val FormatString = "YYYYMMDD"      
    }

    object HOUR extends Variable[String]("hour", classOf[String], Some("Hourly Frequency")) with TemporalVariable {
       override val FormatString = "HH"      
    }

    object MINUTE extends Variable[String]("minute", classOf[String], Some("Hourly Frequency")) with TemporalVariable {
       override val FormatString = "mm"      
         
    }
    
    //// Provide  variable with full timestamp
    object START_TIME extends Variable[String]("start_time", classOf[String], Some("Goal Start time")) with TemporalVariable {
       override val FormatString = "YYYYMMDD hh:mm:ss"
    }
         
}