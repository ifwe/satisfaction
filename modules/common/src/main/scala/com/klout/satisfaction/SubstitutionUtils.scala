package com.klout.satisfaction

import util.matching._
/**
 *  Set of functions for dealing with properties
 */
object SubstitutionUtils {
    val varPrefix = "${"
    val varSuffix = "}"

    val varPattern = new util.matching.Regex("\\$\\{.*?\\}")

    /**
     *
     */
    def findVariablesInString(templateStr: String): Set[String] = {
        val vars = for (m <- varPattern.findAllIn(templateStr)) yield {
            m.substring(2, m.length - 1)
        }

        vars.toSet[String]
    }

    /**
     *  Either return the template string with all the vars returned, or the
     *    vars which ar missing
     */
    def substituteVarsInString(templateStr: String, varMap: Map[String, String]): Either[Set[String], String] = {
        var substStr: String = templateStr
        varMap.foreach{
            case (k, v) =>
                substStr = substStr.replaceAll("\\$\\{" + k + "\\}", v)
        }
        println(" Substitution path is " + substStr)
        var extraVars = findVariablesInString(substStr)
        if (extraVars.size > 0) {
            return Left(extraVars)
        } else {
            return Right(substStr)
        }

    }

}