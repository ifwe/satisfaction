package com.klout.satisfaction

import scalaxb._
import org.specs2.mutable._

class HivePartitionSpec extends Specification {
    val dtParam = new Variable("dt", classOf[String])
    val networkParam = new Variable("network_abbr", classOf[String])

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
            println(" pathExists is " + pathExists)
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

            val checkPart = pathInstance.get.asInstanceOf[HiveTablePartition]
            println(" Last Modified Time is " + checkPart.lastModifiedTime)

        }

        "check cant get bogus DataInstance " in {

            val hiveTbl = new HiveTable("bi_maxwell", "actor_action")

            val witness = new Witness(Substitution((dtParam -> "20030813"),
                (networkParam -> "booger")))

            val pathInstance = hiveTbl.getDataInstance(witness)
            println(" Path instance = " + pathInstance)

            pathInstance mustEqual None
        }

    }

}