package com.klout.satisfaction

import scalaxb._
import org.specs2.mutable._

class HdfsPathSpec extends Specification {

    "VariablePath" should {
        "check if path exists " in {
            val pathTempl = "hdfs://jobs-aa-hnn/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            object dtParam extends Param[String]("dateString")
            object networkParam extends Param[String]("networkAbbr")

            val witness = new Witness(ParamMap((dtParam -> "20130813"),
                (networkParam -> "tw")))
            val pathExists = varPath.exists(witness)
            pathExists must be
        }

        "check if path doesnt exists " in {
            val pathTempl = "hdfs://jobs-aa-hnn/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            object dtParam extends Param[String]("dateString")
            object networkParam extends Param[String]("networkAbbr")

            val witness = new Witness(ParamMap((dtParam -> "20150813"),
                (networkParam -> "horsehead")))
            val pathExists = varPath.exists(witness)
            pathExists must be.not
        }

        "check get DataInstance " in {
            val pathTempl = "hdfs://jobs-aa-hnn/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            object dtParam extends Param[String]("dateString")
            object networkParam extends Param[String]("networkAbbr")

            val witness = new Witness(ParamMap((dtParam -> "20130813"),
                (networkParam -> "tw")))

            val pathInstance = varPath.getDataInstance(witness)

            pathInstance.isDefined must be
            println(" Path is " + pathInstance.get)

            println(" Size is " + hive.ms.Hdfs.prettyPrintSize(pathInstance.get.size))
            println(" LastAccessed is " + pathInstance.get.lastAccessed)
            println(" Created is " + pathInstance.get.created)
        }

        "check cant get bogus DataInstance " in {
            val pathTempl = "hdfs://jobs-aa-hnn/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            object dtParam extends Param[String]("dateString")
            object networkParam extends Param[String]("networkAbbr")

            val witness = new Witness(ParamMap((dtParam -> "20030813"),
                (networkParam -> "booger")))

            val pathInstance = varPath.getDataInstance(witness)

            pathInstance.isEmpty must be
        }

    }

}