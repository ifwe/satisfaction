package com.klout
package satisfaction



import org.specs2.mutable._

class HivePartitionSpec extends Specification {
    val dtParam = new Variable("dt", classOf[String])
    val networkParam = new Variable("network_abbr", classOf[String])

    "HivePartition" should {
        "check if partition exists " in {

            val hiveTbl = new HiveTable("bi_maxwell", "actor_action")

            val witness = new Witness(Substitution((dtParam -> "20131117"),
                (networkParam -> "tw")))
            val pathExists = hiveTbl.exists(witness)
            println(s" PATH EXISTS = $pathExists")
            pathExists mustEqual true
        }
        
        "check if partition doesnt exists dt " in {
            val hiveTbl = new HiveTable("bi_maxwell", "actor_action")

            val witness = new Witness(Substitution((dtParam -> "20150813"),
                (networkParam -> "tw")))
            val pathExists = hiveTbl.exists(witness)
            println(" pathExists is " + pathExists)
            pathExists mustEqual false
        }


        "check if partition doesnt exists " in {
            val hiveTbl = new HiveTable("bi_maxwell", "actor_action")

            val witness = new Witness(Substitution((dtParam -> "20150813"),
                (networkParam -> "horsehead")))
            val pathExists = hiveTbl.exists(witness)
            println(" pathExists is " + pathExists)
            pathExists mustEqual false
        }

        "check get DataInstance " in {
            val hiveTbl = new HiveTable("bi_maxwell", "actor_action")

            val witness = new Witness(Substitution((dtParam -> "20131113"),
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
    
    "HivePartitionGroup" should {
      
      "check if group exists" in {
        
        val partGroup = new HiveTablePartitionGroup("bi_maxwell","ksuid_mapping", Variable("dt"))
        
        val witness = new Witness( Substitution( dtParam -> "20131121"))
        
        val doesExist = partGroup.exists( witness)
        
        doesExist mustEqual true
      }

      "check get partition group" in {
        
        val partGroup = new HiveTablePartitionGroup("bi_maxwell","ksuid_mapping", Variable("dt"))
        
        val witness = new Witness( Substitution( dtParam -> "20131121"))
        
        val dataInstance = partGroup.getDataInstance(witness)
        
        println(s" Partition Group instance is $dataInstance ")
        
        dataInstance.isDefined must be
        
        println(s" Size is ${dataInstance.get.size} ")
        println(s" Created is ${dataInstance.get.created} ")
        println(s" LastAccessed is ${dataInstance.get.lastAccessed} ")
        println(s" Exists is ${dataInstance.get.exists} ")
        
        dataInstance.get.size must beGreaterThan( 1024l )
      }
      
      "check partition group doesn't exist " in {
        val partGroup = new HiveTablePartitionGroup("bi_maxwell","ksuid_mapping", Variable("dt"))
        
        val witness = new Witness( Substitution( dtParam -> "20130221"))
        
        val dataInstance = partGroup.getDataInstance(witness)
        
        println(s" Partition Group instance is $dataInstance ")
        
        dataInstance.isDefined must be.not
        
        
      }
      

    }

}