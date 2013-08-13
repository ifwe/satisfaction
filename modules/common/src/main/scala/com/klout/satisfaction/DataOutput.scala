package com.klout.satisfaction

/**
 *  DataOutput represents an abstract location,
 *    which can produce multiple DataInstances
 */
trait DataOutput extends Evidence {

    def variables: Set[String]
    def exists(params: ParamMap): Boolean
    def getDataInstance(params: ParamMap): Option[DataInstance]

}