package com.klout.satisfaction

import scalaxb._
import org.specs2.mutable._

class HivePartitionSpec extends Specification {
    val dtParam = new Variable("dt", classOf[String])
    val networkParam = new Variable("networkAbbr", classOf[String])

    "HivePartition" should {
        "check if partition exists " in {

            val hiveTbl = new HiveTable("bi_maxwell", "actor_action")

            val witness = new Witness(Substitution((dtParam -> "20130813"),
                (networkParam -> "tw")))
            val pathExists = hiveTbl.exists(witness)
            pathExists must be
        }

        "check if partition doesnt exists " in {
            val hiveTbl = new HiveTable("bi_maxwell", "actor_action")

            val witness = new Witness(Substitution((dtParam -> "20150813"),
                (networkParam -> "horsehead")))
            val pathExists = hiveTbl.exists(witness)
            pathExists must be.not
        }

        "check get DataInstance " in {
            val hiveTbl = new HiveTable("bi_maxwell", "actor_action")

            val witness = new Witness(Substitution((dtParam -> "20130813"),
                (networkParam -> "tw")))

            val pathInstance = hiveTbl.getDataInstance(witness)

            pathInstance.isDefined must be
            println(" Path is " + pathInstance.get)

            println(" Size is " + hive.ms.Hdfs.prettyPrintSize(pathInstance.get.size))
            println(" LastAccessed is " + pathInstance.get.lastAccessed)
            println(" Created is " + pathInstance.get.created)
        }

        "check cant get bogus DataInstance " in {
            val pathTempl = "hdfs://jobs-aa-hnn/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            val witness = new Witness(Substitution((dtParam -> "20030813"),
                (networkParam -> "booger")))

            val pathInstance = varPath.getDataInstance(witness)

            pathInstance.isEmpty must be
        }

    }

}