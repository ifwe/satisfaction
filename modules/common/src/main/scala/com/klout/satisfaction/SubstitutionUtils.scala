package com.klout.satisfaction

import util.matching._
import collection._
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
    def findVariablesInStringXXX(templateStr: String): Set[String] = {
        val vars = for (m <- varPattern.findAllIn(templateStr)) yield {
            m.substring(2, m.length - 1)
        }

        vars.toSet[String]
    }

    /**
     *  Either return the template string with all the vars returned, or the
     *    vars which are missing
     */
    def substituteVarsInStringXXX(templateStr: String, varMap: Map[String, String]): Either[Set[String], String] = {
        var substStr: String = templateStr
        varMap.foreach{
            case (k, v) =>
                val vEscaped = v.replaceAllLiterally("\\$", "\\\\$").
                    replaceAllLiterally("\\{", "\\\\{").
                    replaceAllLiterally("\\}", "\\\\}")

                substStr = substStr.replaceAll("\\$\\{" + k + "\\}", vEscaped)
        }
        println(" Substitution path is " + substStr)
        val extraVars = findVariablesInStringXXX(substStr)
        if (extraVars.size > 0) {
            return Left(extraVars)
        } else {
            return Right(substStr)
        }

    }

}