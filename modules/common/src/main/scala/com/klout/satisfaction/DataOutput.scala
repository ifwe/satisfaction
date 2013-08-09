package com.klout.satisfaction

/**
 *  DataOutput represents an abstract location,
 *    which can produce multiple DataInstances
 */
trait DataOutput extends Evidence {

    def variables: Set[String]
    def exists(witness: Witness): Boolean
    def getDataInstance(witness: Witness): Option[DataInstance]

}