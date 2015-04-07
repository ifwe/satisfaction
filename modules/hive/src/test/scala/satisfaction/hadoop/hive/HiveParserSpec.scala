package satisfaction
package hadoop
package hive

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level
import ms.MetaStore
import fs.FileSystem
import hdfs.Hdfs

@RunWith(classOf[JUnitRunner])
class HiveParserSpec extends Specification {
  
    implicit val ms : MetaStore = MetaStore( new java.net.URI("thrift://jobs-dev-sched2:9085") )
    implicit val hdfs : FileSystem = new Hdfs("hdfs://jobs-dev-hnn") 
  
    "ParserTest" should {
        "parse simple query" in {
            val query = "select ks_uid from actor_action "
              val hiveParser = new HiveParser
            val check = hiveParser.parseSyntax(query)
            check mustEqual true
        }
        "detect bogues query" in {
            val query = "selectfd ks_uid fromi actor_action "
            val hiveParser = new HiveParser
            val check = hiveParser.parseSyntax(query)
            check mustEqual false

        }
        "analyze semantic " in {
            val query = " create table blah as select * from bi_maxwell.actor_action where dt='20130812' and network_abbr='tw' "
            val hiveParser = new HiveParser
            val output = hiveParser.analyzeQuery(query)
            println(" Output is " + output)
        }
        "get lineage " in {
            val query = " insert overwrite table blah select * from actor_action_booger_view where dt='20130812' and network_abbr='tw' "
            val hiveParser = new HiveParser
            hiveParser.getLineage(query)
        }
    }

}