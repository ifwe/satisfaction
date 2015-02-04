package satisfaction.engine

import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.InstrumentedBuilder

/**
 *   Use the Yammer/CodaHale metrics library 
 *    to expose some metrics to JMX
 */
trait Instrumented extends InstrumentedBuilder {
  override val metricRegistry = new com.codahale.metrics.MetricRegistry()

}