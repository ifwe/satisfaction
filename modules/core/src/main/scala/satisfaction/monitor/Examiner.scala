package satisfaction.monitor

import satisfaction.MetricsCollection
import satisfaction.GoalStatus

/**
 *  An Examiner looks at the metrics produced by a 
 *    MetricsProducing task
 *   
 */
trait Examiner {

    def examineMetrics( gs: GoalStatus, mc : MetricsCollection ) : Seq[Aberration]

}