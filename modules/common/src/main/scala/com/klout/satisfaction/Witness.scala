package com.klout.satisfaction

/**
 *  Specialized context class
 *   for Goals
 *
 *   XXX TODO handle special date/time logic
 *   XXX TODO handle variable types
 */
case class Witness(
    variableValues: Map[String, String],
    projectParams: Map[String, String])