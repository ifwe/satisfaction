package satisfaction.monitor

import org.joda.time.DateTime

/**
 *  An Aberration is a departure from what is normal.
 *  
 *  It represents something which is potentially 
 *   wrong, because of some technical issue, 
 *   or an unexpected data-discovery which is
 *   seen from generated data.
 *   
 *   They are generated after a job run, after 
 *    metrics have been collected ,
 *   with declared Examiners after a Goal run.
 *    
 *    
 */
object Severity extends Enumeration {
    type State = Value
    val Trivial,
        Minor,
        Major,
        Severe,
        Retry,
        Fatal = Value
}

case class Aberration( val metricName : String, 
                   val metricVal : Double, 
                   val expectedVal : Double, 
                   val observed : DateTime,
                   val severity : Severity.Value,
                   val explanation : Option[String]) {
  

}