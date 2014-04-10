package com.klout
package satisfaction
package hadoop
package hdfs

import org.specs2.mutable._
import satisfaction.fs._
import java.util.Properties

class HdfsPathSpec extends Specification {
    val dtParam = new Variable("dateString", classOf[String])
    val networkParam = new Variable("networkAbbr", classOf[String])
    

    //// XXX TODO use localfilesystem for unit tests
    implicit val hdfs : FileSystem = Hdfs.fromConfig(HdfsSpec.clientConfig)
    implicit val track : Track = {
      
      val tr = new Track( TrackDescriptor("Test Track"))
      tr.setTrackProperties(new Substitution( Set(Variable( "Bogis") -> "blah")))
      tr
    }
    
    "VariablePath" should {
        "check if path exists " in {
            val pathTempl = "hdfs://nameservice1/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            val witness = Witness(Substitution((dtParam -> "20130821"),
                (networkParam -> "tw")))
            val pathExists = varPath.exists(witness)
            pathExists must be
        }

        "check if path doesnt exists " in {
            val pathTempl = "hdfs://nameservice1/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            val witness = new Witness(Substitution((dtParam -> "20150813"),
                (networkParam -> "horsehead")))
            val pathExists = varPath.exists(witness)
            pathExists must be.not
        }

        "check get DataInstance " in {
            val pathTempl = "hdfs://nameservice1/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            val witness = new Witness(Substitution((dtParam -> "20130813"),
                (networkParam -> "tw")))

            val pathInstance = varPath.getDataInstance(witness)

            pathInstance.isDefined must be
            println(" Path is " + pathInstance.get)

            println(" Size is " + Hdfs.prettyPrintSize(pathInstance.get.size))
            println(" LastAccessed is " + pathInstance.get.lastAccessed)
            println(" Created is " + pathInstance.get.created)
        }

        "check cant get bogus DataInstance " in {
            val pathTempl = "hdfs://nameservice1/data/hive/maxwell/actor_action/${dateString}/${networkAbbr}"

            val varPath = new VariablePath(pathTempl)

            val witness = new Witness(Substitution((dtParam -> "20030813"),
                (networkParam -> "booger")))

            val pathInstance = varPath.getDataInstance(witness)

            pathInstance.isEmpty must be
        }

    }

}