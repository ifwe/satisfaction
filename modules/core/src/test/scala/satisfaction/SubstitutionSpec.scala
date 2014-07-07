package com.klout.satisfaction

import org.specs2.mutable._
import scala.util.parsing.input.CharSequenceReader
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class WitnessSpec extends Specification {

    "WitnessUtils" should {
        "find variables in string " in {
            val str = " select * from my_view_${networkAbbr} where dt= ${dateString}  "

            val vars = Substituter.findVariablesInString(str)
            vars.foreach(str => println(str))

            vars must contain("networkAbbr")
            vars must contain("dateString")
        }
        "substitute variables in string " in {
            val tempStr = " select * from my_view_${networkAbbr} where dt= ${dateString}"

            val varMap = Map(("networkAbbr" -> "tw"), ("dateString" -> "20130813"))

            val str = Substituter.substitute(new CharSequenceReader(tempStr), Witness(varMap))
            str.isRight must be

            println(" Substr string is " + str)
            str match {
                case Right(substituted) =>
                    substituted mustEqual " select * from my_view_tw where dt= 20130813"
            }
        }
        "handle missing curly brace " in {
            val tempStr = " select * from my_view_${networkAbbr where dt= ${dateString}"

            val varMap = Map(("networkAbbr" -> "tw"), ("dateString" -> "20130813"))

            val str = Substituter.substitute(new CharSequenceReader(tempStr), Witness(varMap))
            str.isLeft must be

            str match {
                case Left(missingVars) =>
                    missingVars.foreach(v => println(" Missing var " + v))
                    missingVars.size mustEqual 1
                    missingVars must contain("networkAbbr")
            }
        }
        " handle dollar signs without curly " in {
            val tempStr = " select $nonCurly from my_view_${networkAbbr} where dt= ${dateString}"

            val varMap = Map(("networkAbbr" -> "tw"), ("dateString" -> "20130813"))

            val str = Substituter.substitute(new CharSequenceReader(tempStr), Witness(varMap))
            str.isRight must be

            println(" Substr string is " + str)
            str match {
                case Right(substituted) =>
                    substituted mustEqual " select $nonCurly from my_view_tw where dt= 20130813"
            }
        }
        "detect unsubstituted variables in string " in {
            val tempStr = " select * from my_view_${networkAbbr} where dt= ${dateString}" +
                " and ks_uid = ${ksUid} and actor_id = ${actorId}  "

            val varMap = Map(("networkAbbr" -> "tw"), ("dateString" -> "20130813"))
            val str = Substituter.substitute(new CharSequenceReader(tempStr), Witness(varMap))
            str.isLeft must be

            str match {
                case Left(missingVars) =>
                    missingVars.foreach(v => println(" Missing var " + v))
                    missingVars.size mustEqual 2
                    missingVars must contain("ksUid")
                    missingVars must contain("actorId")
            }
        }

        "Read Property file" in {
            val goodProps = Substituter.readProperties("modules/core/src/test/resources/goodset.properties")

            goodProps.keySet must contain("myProp")
            goodProps.keySet must contain("theBigProp")

            goodProps.get("myProp").get mustEqual "myVal"
            goodProps.get("theBigProp").get mustEqual "12244"

        }

        "Subst vars in Property file" in {
            val goodProps = Substituter.readProperties("modules/core/src/test/resources/subst_var.properties")

            goodProps.keySet must contain("nameNode")
            goodProps.keySet must contain("dataRoot")
            ///goodProps.keySet must contain("myTablePath")

            println(" NameNode is " + goodProps.get("nameNode").get)
            println(" DataRoot is " + goodProps.get("dataRoot").get)
            ///println(" MyTablePath is " + goodProps.get("myTablePath").get)
            //goodProps.get("myProp").get mustEqual "myVal"
            //goodProps.get("theBigProp").get mustEqual "12244"

        }
        
        "implicitly convert to java.util.Properties" in {
            val goodProps = Substituter.readProperties("modules/core/src/test/resources/goodset.properties")
            val subst = Witness( goodProps)
            
            val javaProps : java.util.Properties = subst
            
            subst.raw foreach { case(k,v) => {
                  val lookup = javaProps.getProperty( k)
                  println(s"  Lookup for key $k is $lookup ")  
                   ( lookup must not beNull )
                  lookup must be(v)
              }
            } 
          
        }
        
        
        "parse dauDB" in {
          val pageViewQuery = " use ${dauDB}; "
            
            val props= new java.util.Properties
            
            props.put("dauDB", "ramblas")
            
            val parsed = Substituter.substitute( pageViewQuery , props)
            
            parsed match {
            case Left[missingVariables]
            
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

        "create typed Variable assignments" in {
            val intAss = VariableAssignment("IntProperty", 1)
            intAss.variable.clazz mustEqual classOf[Int]
            intAss.variable.name mustEqual "IntProperty"
            intAss.value mustEqual 1

            val boolAss = VariableAssignment("BooleanProperty", true)
            boolAss.variable.clazz mustEqual classOf[Boolean]
            boolAss.variable.name mustEqual "BooleanProperty"
            boolAss.value mustEqual true

        }

        "Substituion should get and update" in {
            val subst1 = Witness(VariableAssignment("FirstProp", "FirstVal"),
                VariableAssignment("NumericVal", 3.14159)
            )
            val checkLookup = subst1.get(Variable("FirstProp")).get
            println(" Value is " + checkLookup)
            checkLookup mustEqual "FirstVal"
        }

        /// XXX JDB  FIX ME
        "Qualify witness function" in {
            val subst1 = Witness(VariableAssignment("dt", "20130917"))
            val mapFunc = Goal.qualifyWitness(Variable("tableAlias"), "friends")

            val subst2 = mapFunc(subst1)
            println(" qualified witness is " + subst2)
            subst2.assignments.foreach(println)

            subst2.assignments.size mustEqual 2
        }

        "compose Qualify witness function" in {
            val subst1 = Witness(VariableAssignment("dt", "20130917"))
            val mapFunc1 = Goal.qualifyWitness(Variable("tableAlias"), "friends")
            val mapFunc2 = Goal.qualifyWitness(Variable("graphType"), "TWITTER_FRIENDS")
            val mapFunc = mapFunc1 compose mapFunc2

            val subst2 = mapFunc(subst1)
            println(" qualified witness is " + subst2)
            subst2.assignments.foreach{ ass => println("COMPOSED " + ass) }

            subst2.assignments.size mustEqual 3
        }
        
        "mapVariables function" in {
           val subst1 = new Witness(Set( VariableAssignment("dt" , "20140512"),
                                     VariableAssignment("hour" , "03" ),
                                     VariableAssignment("minute" , "43" )))
           
           val mapVarFunc = Witness.mapVariables( Variable("dt"), Variable("date"))_
           val subst2 = mapVarFunc(subst1)
          
           println(s" Witness with mapped variable is $subst2")
           
           subst2.assignments.size mustEqual 3
           
           subst2.variables must contain( Variable("date") )
           subst2.variables must not contain( Variable("dt") )
           subst2.variables must contain( Variable("hour") )
           subst2.variables must contain( Variable("minute") )
        }

    }

}