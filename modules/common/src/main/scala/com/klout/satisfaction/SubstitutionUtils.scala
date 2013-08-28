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
    def findVariablesInString(templateStr: String): Set[String] = {
        val vars = for (m <- varPattern.findAllIn(templateStr)) yield {
            m.substring(2, m.length - 1)
        }

        vars.toSet[String]
    }

    /**
     *  Either return the template string with all the vars returned, or the
     *    vars which are missing
     */
    def substituteVarsInString(templateStr: String, varMap: Map[String, String]): Either[Set[String], String] = {
        var substStr: String = templateStr
        varMap.foreach{
            case (k, v) =>
                val vEscaped = v.replaceAllLiterally("\\$", "\\\\$").
                    replaceAllLiterally("\\{", "\\\\{").
                    replaceAllLiterally("\\}", "\\\\}")

                substStr = substStr.replaceAll("\\$\\{" + k + "\\}", vEscaped)
        }
        println(" Substitution path is " + substStr)
        val extraVars = findVariablesInString(substStr)
        if (extraVars.size > 0) {
            return Left(extraVars)
        } else {
            return Right(substStr)
        }

    }

    /**
     *  Substitute any variables
     */
    def substituteVarsInMap(varMap: Map[String, String]): Map[String, String] = {
        varMap.collect {
            case (k, v) =>
                substituteVarsInString(v, varMap) match {

                    case Left(extraVars) =>
                        /// There are some missing variables which need to be defined elsewhere ...
                        throw new IllegalArgumentException(s" Missing variables is property $k with value $v -- " + extraVars)
                    case Right(line) =>
                        (k -> line)
                }
        }
    }

    def readProperties(propertyFile: String): Map[String, String] = {
        val fileProps = io.Source.fromFile(propertyFile).getLines.filterNot(_.isEmpty).
            filterNot(_.startsWith("#")).collect{ case s: String => parseLine(s) }.toMap

        substituteVarsInMap(fileProps)
    }

    def parseLine(propertyString: String): Tuple2[String, String] = {
        val property: Array[String] = propertyString.split("=")
        if (property.length != 2)
            throw new RuntimeException("error: property file not correctly formatted")

        property(0).trim -> property(1).trim
    }

}