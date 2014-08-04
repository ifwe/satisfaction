package satisfaction
package track

import org.joda.time.Period
import org.joda.time.PeriodType
import org.joda.time.Partial
import org.joda.time._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISOPeriodFormat
import org.joda.time.format.PeriodFormatter
import org.joda.time.format.PeriodFormat
import org.joda.time.format.DateTimeFormat

case class SLA(slaType: String, timeOfPeriod: ReadablePartial, frequency: Period) {
    val dailyFormatter = ISODateTimeFormat.basicTime()

    @Override
    override def toString(): String = {
        val periodStr = SLA.periodFormatter.print(frequency)
        val partialStr = SLA.getPartialFormatterForPeriod(frequency).print(timeOfPeriod)

        slaType + "," + partialStr + "," + periodStr
    }
}

object SLA {
    val periodFormatter: PeriodFormatter = ISOPeriodFormat.standard

    def fromString(slaStr: String): SLA = {
        val split: Array[String] = slaStr.split(',')

        val periodStr = split(2)
        val period = periodFormatter.parsePeriod(split(2))

        val formatter = getPartialFormatterForPeriod(period)
        val partDT = formatter.parseDateTime(split(1))
        val part = new LocalTime(partDT.hourOfDay.get(), partDT.minuteOfHour.get())

        return new SLA(split(0).toString, part, period)

    }

    def getPartialFormatterForPeriod(period: Period): DateTimeFormatter = {
      /// XXX Get different formatter for day of month...
        return DateTimeFormat.forPattern("HHmm")
    }

}

