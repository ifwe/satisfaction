package com.klout.satisfaction

/**
 *  Specialized context class
 *   for Goals
 *
 *   XXX TODO handle special date/time logic
 *   XXX TODO handle variable types
 */
case class Witness(variables: ParamMap) {

    lazy val params: Map[String, String] = ???

    def update[T](paramPair: ParamPair[T]): Witness = this copy (variables update paramPair)
}