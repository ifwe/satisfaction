package com.klout
package satisfaction
package hadoop
package hive.ms

import org.specs2.mutable._
import com.klout.satisfaction.Witness
import com.klout.satisfaction._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class HiveTableSpec extends Specification {
    val dtParam = new Variable("dt", classOf[String])
    val networkParam = new Variable("network_abbr", classOf[String])
    val featureGroup = new Variable[Int]("service_id", classOf[Int])

    "HiveTable" should {
        "provide variables" in {
            val actorAction = new HiveTable("bi_maxwell", "actor_action")
            val params = actorAction.variables
            val dtVar = Variable[String]("dt", classOf[String])
            params.foreach(p =>
                println (" Parameter is " + p.name)
            )

            params.size must_== 2
            params must contain(Variable[String]("dt", classOf[String]))
            params must contain(Variable[String]("network_abbr", classOf[String]))
        }
        "implements exists" in {
            val actorAction = new HiveTable("bi_maxwell", "actor_action")
            val witness = new Witness(new Substitution(Set((dtParam -> "20130812"),
                (networkParam -> "tw"))))

            val xist = actorAction.exists(witness)
            if (xist) {
                println("  Witness exists ")
            } else {
                println(" Witness doesn't exist")
            }

            xist must be
        }
        "ksuid_mapping exists" in {
            val ksuid_mapping = new HiveTable("bi_maxwell", "ksuid_mapping")
            val witness = new Witness(new Substitution(Set((dtParam -> "20130821"), (featureGroup -> 1))))

            val dataInstance = ksuid_mapping.getDataInstance(witness)
        }
        "ksuid_mapping doesnt exists" in {
            val ksuid_mapping = new HiveTable("bi_maxwell", "ksuid_mapping")
            val witness = new Witness(new Substitution(Set((dtParam -> "20190821"), (featureGroup -> 1))))

            val doesNotExist = ksuid_mapping.exists(witness)

            (!doesNotExist) must be
        }

    }

}