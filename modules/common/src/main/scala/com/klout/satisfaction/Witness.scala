package com.klout.satisfaction

/**
 *  Specialized context class
 *   for Goals
 *
 *   XXX TODO handle special date/time logic
 *   XXX TODO handle variable types
 */
case class Witness(params: ParamMap) {

    lazy val variables: Map[String, String] = params.raw

    def update[T](paramPair: ParamPair[T]): Witness = this copy (params + paramPair)
}

object Witness {
    def apply(pairs: ParamPair[_]*): Witness = Witness(ParamMap(pairs: _*))
}