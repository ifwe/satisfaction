package satisfaction

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISOPeriodFormat
import org.joda.time.Period
import org.joda.time.Partial
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.joda.time.LocalDateTime


/**
 *  Associate a notion of Time for certain variables
 */
trait TemporalVariable { self : Variable[String] => 

    val formatString :String
    val frequency : Period
  
    def formatted( dt : DateTime) : String = {
       formatter.print(dt)
    }
   
    def parsed( str : String ) : DateTime = {
       formatter.parseDateTime(str) 
    }
    
    lazy val formatter : DateTimeFormatter = DateTimeFormat.forPattern( formatString)
    
    {
       TemporalVariable.register( this)
    }
    
}
    
 object TemporalVariable {
    import Temporal._
    
    private val _tvMap : scala.collection.mutable.Map[String,TemporalVariable]  =  scala.collection.mutable.HashMap.empty

    def register( tv :TemporalVariable ) = {
      _tvMap.put(tv.asInstanceOf[Variable[_]].name, tv)
    }
    
    def isTemporalVariable( v : Variable[_] ) : Option[TemporalVariable] = {
      v match {
        case tv : TemporalVariable => Some(tv) 
        case vStr : Variable[String] => _tvMap.get( vStr.name)
        case _ => None
      }
    }
    
    
    object Dt extends Variable[String]("dt", classOf[String], Some("Daily Frequency")) with TemporalVariable {
       override val formatString =  dailyFormat
       override val frequency =   dailyPeriod
    }

    /// Alternative Daily Frequency VAr
    object Date extends Variable[String]("date", classOf[String], Some("Alternative Daily Frequency Varr")) with TemporalVariable {
       override val formatString =  dailyFormat
       override val frequency = dailyPeriod
    }

    object Hour extends Variable[String]("hour", classOf[String], Some("Hourly Frequency")) with TemporalVariable {
       override val formatString =  hourlyFormat
       override val frequency = hourlyPeriod
    }

    object Minute extends Variable[String]("minute", classOf[String], Some("Minute Frequency")) with TemporalVariable {
       override val formatString =  minuteFormat
       override val frequency =  minutePeriod
    }
    
    object StartTime extends Variable[String]("start_time", classOf[String], Some("Goal Start time")) with TemporalVariable {
       override val formatString = timestampFormat 
       override val frequency =  continuousFrequency
    }
    
    
    def dateTimeForWitness( w: Witness ) : DateTime = {
      val localDates : Seq[LocalDateTime] = w.variables.map( v => {
        isTemporalVariable(v) match { 
          case Some(tv) => {
            val dtStr : String = w.get( tv.asInstanceOf[Variable[String]]).get.toString
            val formatter = DateTimeFormat.forPattern( tv.formatString)
            Some(formatter.parseLocalDateTime( dtStr))
          } 
          case None => None
        }
      } ).filter( _.isDefined).map( _.get).toSeq
     
      
      val mergeDate : LocalDateTime = localDates.foldLeft( new LocalDateTime() )( (ldt, mdt) => {
        ldt.withFields( mdt)
      } )
        
      mergeDate.toDateTime
   }

}

 object Temporal {
  
    def frequency( freq : String) =  { ISOPeriodFormat.standard.parsePeriod(freq) }
  
    val dailyFormat = "YYYYMMdd"
    val dailyPeriod =  frequency("P1D")

    val hourlyFormat =  "HH"
    val hourlyPeriod =  frequency("PT1H")
    
    val minuteFormat =  "mm"
    val minutePeriod =  frequency("PT1M")
      
      
    val timestampFormat = "YYYYMMddHHmmss"
    val continuousFrequency = frequency("PT1S")
    
    /**
     *  Provide a sequence which produces the hours of the day in a preferred format
     */
    def hours( startHour: Int , endHour : Int ) : Iterable[String] = {
       ( startHour to endHour ) map ( hr => { 
          new java.text.DecimalFormat("00").format(hr)
         }  
       )
    }

    def hours : Iterable[String] = {
      hours( 0, 23)
    }

    def hours( startTime : DateTime , endTime : DateTime ) : Iterable[String] = {
       hours( startTime.hourOfDay.get , endTime.hourOfDay.get ) 
    }
            
    
    class TimeIterator( val startTime: DateTime, val endTime: DateTime, val period:Period) 
          extends Iterator[DateTime] {
        var dt : DateTime = null

        override def hasNext : Boolean = {
          if(dt == null)
             dt = startTime
           else 
             dt = dt.plus(period)
           dt.isBefore( endTime)
        } 
        
        override def next : DateTime = {
          if(dt == null)
             dt = startTime
         if( dt.isBefore(endTime))
            dt
          else 
             null /// throw  exception
        }
    }
    
    class TimeIterable( val startTime : DateTime, val endTime : DateTime, val period : Period ) extends Iterable[DateTime]{
       override def iterator =  { 
          new TimeIterator( startTime, endTime, period)  }
    }
    
    
    /**
     *  TimeRange inclusive 
     *   produces sequence like 20140429,20140430,20140431,20140501,20140502
     */
    def timeRange( startTime : DateTime, endTime : DateTime)
        ( formatString : String , period : Period) : Iterable[String] = {
      val formatter =  DateTimeFormat.forPattern(formatString)
      (new TimeIterable( startTime, endTime, period)) map { formatter.print( _ ) }
    }
    
    def timeRange( startTimeStr : String,  endTimeStr : String)
        ( formatString : String , period : Period) : Iterable[String] = {
      val formatter =  DateTimeFormat.forPattern(formatString)
      val sd = formatter.parseDateTime( startTimeStr)
      val ed = formatter.parseDateTime( endTimeStr)
      (new TimeIterable( sd, ed , period)).
          map { formatter.print( _ ) }
    }
    

    def dateRange( startDay : DateTime, endDay : DateTime ) : Iterable[String] = 
        timeRange(startDay, endDay)( dailyFormat, dailyPeriod) 
      
    def dateRange( startDayStr : String, endDayStr : String ) : Iterable[String] = 
        timeRange(startDayStr, endDayStr)( dailyFormat, dailyPeriod)
            
         
}


object ForDateRange {
  
    //// XXX For now need to know date range at compile time, for fanout..
    def apply( subGoal : Goal , startDate : String, endDate : String ) : Goal = {
       FanOutGoal( subGoal, TemporalVariable.Date, Temporal.dateRange( startDate,endDate) )
      
    }
  
}

    