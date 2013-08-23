package hive.ms

import scalaxb._
import org.specs2.mutable._
import com.klout.satisfaction.HiveTable
import com.klout.satisfaction.Witness
import com.klout.satisfaction._

class HiveTableSpec extends Specification {
    val dtParam = new Variable("dt", classOf[String])
    val networkParam = new Variable("network_abbr", classOf[String])
    val featureGroup = new Variable[Int]("service_id", classOf[Int])

    "HiveTable" should {
        "provide variables" in {
            val actorAction = new HiveTable("bi_maxwell", "actor_action")
            val params = actorAction.variables
            params.foreach(p =>
                println(" Parameter is " + p.name)
            )
        }
        "implements exists" in {
            val actorAction = new HiveTable("bi_maxwell", "actor_action")
            val witness = new Witness(new Substitution(Set((dtParam -> "20130812"),
                (networkParam -> "tw"))))

            if (actorAction.exists(witness)) {
                println("  Witness exists ")
            } else {
                println(" Witness doesn't exist")
            }
        }
        "ksuid_mapping exists" in {
            val ksuid_mapping = new HiveTable("bi_maxwell", "ksuid_mapping")
            val witness = new Witness(new Substitution(Set((dtParam -> "20130821"), (featureGroup -> 1))))

            val dataInstance = ksuid_mapping.getDataInstance(witness)
        }
    }

}