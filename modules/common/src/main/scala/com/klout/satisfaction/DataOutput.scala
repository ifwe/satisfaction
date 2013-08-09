package com.klout.satisfaction

/**
 *  DataOutput represents an abstract location,
 *    which can produce multiple DataInstances
 */
abstract class DataOutput {

    def getVariables : Set[String]
    def instanceExists(witness: Witness): Boolean
    def getDataInstance(witness: Witness): DataInstance

}