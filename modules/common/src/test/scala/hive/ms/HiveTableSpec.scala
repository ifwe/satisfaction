package hive.ms

import scalaxb._
import org.specs2.mutable._
import com.klout.satisfaction.HiveTable
import com.klout.satisfaction.Witness
import com.klout.satisfaction.ParamMap
import com.klout.satisfaction._

class HiveTableSpec extends Specification {
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
            object NetworkAbbr extends Param[String]("network_abbr")
            object date extends Param[String]("dt")
            val witness = new Witness(new ParamMap(Set((date -> "20130812"),
                (NetworkAbbr -> "tw"))))

            if (actorAction.exists(witness)) {
                println("  Witness exists ")
            } else {
                println(" Witness doesn't exist")
            }
        }
    }

}