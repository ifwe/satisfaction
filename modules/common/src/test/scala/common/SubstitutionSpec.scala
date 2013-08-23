package com.klout.satisfaction

import scalaxb._
import org.specs2.mutable._

class SubstitutionSpec extends Specification {

    "SubstitutionUtils" should {
        "find variables in string " in {
            val str = " select * from my_view_${networkAbbr} where dt= ${dateString}  "

            val vars = SubstitutionUtils.findVariablesInString(str)
            vars.foreach(str => println(str))

            vars must contain("networkAbbr")
            vars must contain("dateString")
        }
        "substitute variables in string " in {
            val tempStr = " select * from my_view_${networkAbbr} where dt= ${dateString}"

            val varMap = Map(("networkAbbr" -> "tw"), ("dateString" -> "20130813"))
            val str = SubstitutionUtils.substituteVarsInString(tempStr, varMap)
            str.isRight must be

            println(" Substr string is " + str)
            str match {
                case Right(substituted) =>
                    substituted mustEqual " select * from my_view_tw where dt= 20130813"
            }
        }
        "detect unsubstituted variables in string " in {
            val tempStr = " select * from my_view_${networkAbbr} where dt= ${dateString}" +
                " and ks_uid = ${ksUid} and actor_id = ${actorId}  "

            val varMap = Map(("networkAbbr" -> "tw"), ("dateString" -> "20130813"))
            val str = SubstitutionUtils.substituteVarsInString(tempStr, varMap)
            str.isLeft must be

            str match {
                case Left(missingVars) =>
                    missingVars.foreach(v => println(" Missing var " + v))
                    missingVars.size mustEqual 2
                    missingVars must contain("ksUid")
                    missingVars must contain("actorId")
            }
        }
    }

    "Witness creation" should {
        "create witness from variables" in {

            val dtVar = new Variable("dt", classOf[Int])
            val netVar = new Variable("network_abbr", classOf[String])
            val ass1 = VariableAssignment[Int](dtVar, 2323)
            val ass2 = VariableAssignment[String](new Variable("network_abbr", classOf[String]), "twitter")
            //val witness = Witness( VariableAssignment("network", "twitter"),
            //VariableAssignment[Int]("service_id", 1) )
            val witness = Witness(ass1, ass2)

            val vars = witness.variables
            println("Witness is  " + witness)

            vars.foreach(s => println(" Var is " + s))

            vars must contain(dtVar)
            vars must contain(netVar)

        }

    }

}