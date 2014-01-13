package com.klout
package satisfaction

/**
 *  DataOutput represents an abstract location,
 *    which can produce multiple DataInstances
 */
trait DataOutput extends Evidence {

    def variables: Set[Variable[_]]
    def exists(witness: Witness): Boolean
    def getDataInstance(witness: Witness): Option[DataInstance]

}