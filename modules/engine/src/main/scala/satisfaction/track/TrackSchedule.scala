package com.klout
package satisfaction
package track

import org.joda.time.LocalTime
import org.joda.time.Period
import com.klout.satisfaction.engine.actors._
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISOPeriodFormat

  
  /**
   *  A TrackSchedule represents when the scheduler starts the Track, and 
   *   begins to attempt the top level goal for the tracks.
   *  ( Of course, the goals will block until all downstream goals have been satisfied) 
   *  
   *   Optionally, one can add a default Set of SLA's for the Track, for ultimate 
   *    completion of the top level goals
   */
  case class TrackSchedule( startTime : LocalTime, freq : Period, slas : Set[SLA] = Set.empty) {
    
    
    def getCronString : String = {
      //// From the Period and start time,
      ///// Generate the "CRON" string
      //// For now, assume only daily frequency   
      var sb : StringBuilder  = new StringBuilder
      sb ++= startTime.getSecondOfMinute.toString
      sb += ' '
      sb ++= startTime.getMinuteOfHour.toString
      sb += ' '
      sb ++= startTime.getHourOfDay.toString
      sb ++= " * * ? *"
      
      sb.toString  
    }
  }
  
  object TrackSchedule{ 
    val formatter : DateTimeFormatter = DateTimeFormat.forPattern("HH:mm")
    
    def apply( formatStr : String) : TrackSchedule = {
      val splitArr = formatStr.split(";")
      val stTime = formatter.parseLocalTime(splitArr(0))
      val per = ISOPeriodFormat.standard.parsePeriod( splitArr(1))
     
      new TrackSchedule( stTime, per)
    }
    
  }
  