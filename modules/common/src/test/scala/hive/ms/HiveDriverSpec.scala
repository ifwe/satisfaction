package hive.ms

import scalaxb._
import org.specs2.mutable._

class HiveDriverSpec extends Specification {
    "DriverTest" should {
        "analyze semantic " in {
            HiveClient.useDatabase("bi_maxwell")
            val query = " create table blah as select * from bi_maxwell.actor_action where dt='20130812' and network_abbr='tw' "

            val output = HiveClient.executeQuery(query)
            println(" Output is " + output)
        }
    }

}