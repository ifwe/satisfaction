package com.klout.satisfaction

import org.joda.time._

case class WitnessGenerator(
    cronOverride: Option[String],
    generator: Option[Witness] => Option[Witness])

object WitnessGenerator {

    def apply(f: Option[Witness] => Option[Witness]): WitnessGenerator = WitnessGenerator(None, f)

    def apply(cron: String)(f: Option[Witness] => Option[Witness]): WitnessGenerator = WitnessGenerator(Some(cron), f)

    def daily(cron: String, dateParam: Param[LocalDate]): WitnessGenerator = {
        def today(): LocalDate = new LocalDate(DateTimeZone.UTC)

        WitnessGenerator(cron) {
            case Some(current) =>
                current.params get dateParam match {
                    case Some(date) if date == today() => None
                    case _                             => Some(Witness(dateParam -> today()))
                }
            case None => Some(Witness(dateParam -> today()))
        }
    }

}

