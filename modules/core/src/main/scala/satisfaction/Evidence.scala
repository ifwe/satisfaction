package com.klout
package satisfaction

/**
 *  Provides evidence that a Goal 
 *    has been satisfied.
 */
trait Evidence {

    /**
     * Return true if the Goal has been 
     *    satisfied for the specified Witness
     */
    def exists(w: Witness): Boolean

}


/**
 *  An Evidence which would never be satisfied
 */
object NeverSatisfied extends Evidence {
    override def exists(w: Witness) : Boolean = { false }
}

object AlwaysSatisfied extends Evidence {
    override def exists(w: Witness) : Boolean = { true }
}

