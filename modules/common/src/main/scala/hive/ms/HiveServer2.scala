package hive.ms

import org.apache.hadoop.hive.conf.HiveConf
import org.joda.time.DateTime

/**
 *  Scala wrapper around  embedded HiveServer2
 */
class HiveServer2 {
    implicit val config: HiveConf = Config.config

    lazy val server = {
        val s = new org.apache.hive.service.server.HiveServer2
        config.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-dev-sched2:9084")
        config.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, "1111")

        s.init(config)

        s.start
        println(" Starting Server " + s.getName() + " started at "
            + new DateTime(s.getStartTime) + " with state " + s.getServiceState())
        //// Block for a while until the server starts
        Thread.sleep(4000)
        s
    }

    def cliService = server.getServices()
}

object HiveServer2 extends HiveServer2 {
    def main(argv: Array[String]): Unit = {

        val startup = server

        println(" Starting Server " + startup.getName() + " started at " + startup.getStartTime + " with state " + startup.getServiceState())

        Thread.sleep(30000)
    }

}
