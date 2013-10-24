package com.klout.satisfaction
package executor
package track

import org.joda.time.LocalTime
import org.joda.time.Period
import hive.ms.SLA
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISOPeriodFormat

/**
   *  Case class describing how a track can be deployed.
   *  There might be multiple versions of a track, by different users,
   *   for different possible variants.
   *   
   *   For example, in a staging or dev environment, multiple users could be
   *    working on the same track, possibly with multiple features or variants.
   *   In a production system, one could imagine multiple tracks, running in parallel,
   *    in order to compare results before releasing.
   */
  case class TrackDescriptor( val trackName : String, val forUser : String, val version : String, variant : Option[String] = None) {
   
     override def toString() = {
       s"TrackDescriptor::name=$trackName forUser=$forUser Version= $version Variant=$variant"
     }
 }
  
  object TrackDescriptor  {  
     def apply( tName : String ) : TrackDescriptor = {
        new TrackDescriptor( tName, tName, "LATEST", None) 
     }
  }
  
  
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
  