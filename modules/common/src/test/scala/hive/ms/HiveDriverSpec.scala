package hive.ms

import scalaxb._
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level

@RunWith(classOf[JUnitRunner])
class HiveDriverSpec extends Specification {

    "DriverTest" should {
        "access hive client " in {

            ///val log = Logger.getLogger( classOf[org.apache.hadoop.mapreduce.Cluster])
            val log = Logger.getLogger("org.apache.hadoop")
            log.setLevel(Level.DEBUG)

            val showDB = HiveClient.executeQuery("show databases")
            val weird = HiveClient.executeQuery("SET yarn.resourcemanager.address=\"foobar\";")
            HiveClient.useDatabase("bi_maxwell")
            val showTbls = "show tables"
            val out1 = HiveClient.executeQuery(showTbls)
            val query = " create table blah as select * from bi_maxwell.actor_action where dt='20130812' and network_abbr='tw' "

            val output = HiveClient.executeQuery(query)
            println(" Output is " + output)
        }
    }

}