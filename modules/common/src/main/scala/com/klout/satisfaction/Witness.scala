package com.klout.satisfaction

/**
 *  Specialized context class
 *   for Goals
 *
 *   XXX TODO handle special date/time logic
 *   XXX TODO handle variable types
 */
case class Witness(val variables: ParamMap) {

    def apply(tuples: Set[Tuple2[String, String]]): Witness = {

        null
    }

    def apply(fullMap: ParamMap, variables: Set[Param[_]]): Witness = {
        null
    }

    lazy val params: Map[String, String] = {
        variables.raw
    }

    def update[T](paramPair: ParamPair[T]): Witness = this copy (variables update paramPair)
}