package com.klout
package satisfaction
package hadoop
package hive.ms

import org.specs2.mutable._
import com.klout.satisfaction.Witness
import com.klout.satisfaction._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import satisfaction.fs.FileSystem
import satisfaction.hadoop.hdfs.Hdfs

@RunWith(classOf[JUnitRunner])
class HiveTableSpec extends Specification {
    val dtParam = new Variable("dt", classOf[String])
    val hourParam = new Variable("hour", classOf[String])
    val networkParam = new Variable("network_abbr", classOf[String])
    val featureGroup = new Variable[Int]("service_id", classOf[Int])

    //// XXX Externalize  ---
    //// Remove references to Klout ( or Tagged)
    implicit  val ms : MetaStore = MetaStore(new java.net.URI("thrift://dhdp2jump01:9083"))
    implicit  val hdfs : FileSystem = Hdfs.default
    
    "HiveTable" should {
        "provide variables" in {
            val dauByPlatform = new HiveTable("ramblas", "dau_by_platform")
            val params = dauByPlatform.variables
            val dtVar = Variable[String]("dt", classOf[String])
            params.foreach(p =>
                println (" Parameter is " + p.name)
            )

            params.size must_== 2
            params must contain(Variable[String]("dt", classOf[String]))
            params must contain(Variable[String]("hour", classOf[String]))
        }
        "implements exists" in {
            val dauByPlatform = new HiveTable("ramblas", "dau_by_platform")
            val witness = new Witness(Set((dtParam -> "20140512"),
                (hourParam -> "03")))

            val xist = dauByPlatform.exists(witness)
            if (xist) {
                println("  Witness exists ")
            } else {
                println(" Witness doesn't exist")
            }

            xist must_== true
        }
        "partitiion doesnt exists" in {
            val dauByPlatform = new HiveTable("ramblas", "dau_by_platform")
            val witness = new Witness(Set((dtParam -> "20190821"), (hourParam -> "05")))

            val doesNotExist = dauByPlatform.exists(witness)

            doesNotExist must_== false
        }

    }

}