package com.klout
package satisfaction
package hadoop
package hive

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level

@RunWith(classOf[JUnitRunner])
class HiveParserSpec extends Specification {
    "ParserTest" should {
        "parse simple query" in {
            val query = "select ks_uid from actor_action "
            val check = HiveParser.parseSyntax(query)
            check mustEqual true
        }
        "detect bogues query" in {
            val query = "selectfd ks_uid fromi actor_action "
            val check = HiveParser.parseSyntax(query)
            check mustEqual false

        }
        "analyze semantic " in {
            val query = " create table blah as select * from bi_maxwell.actor_action where dt='20130812' and network_abbr='tw' "
            val output = HiveParser.analyzeQuery(query)
            println(" Output is " + output)
        }
        "get lineage " in {
            val query = " insert overwrite table blah select * from actor_action_booger_view where dt='20130812' and network_abbr='tw' "
            HiveParser.getLineage(query)
        }
    }

}