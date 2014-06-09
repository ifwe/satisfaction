package com.klout
package satisfaction
package hadoop
package hive

import java.sql._

import org.apache.hive.jdbc._
import org.apache.hadoop.hive.conf.HiveConf


/**
 * Executes jobs remotely ...
 * 
 *   XXX  Can we support 
 *     HiveServer2 ???
 *    Can we add code (UDF's) 
 *     to running server ??? 
 * 
 */
class HiveClient(val jdbcDriverUrl: String) extends HiveDriver {
    val driverClass = Class.forName("org.apache.hive.jdbc.HiveDriver")
    /// 
    //// class to startup embedded HiveServer2 instance
    ///implicit val server = HiveServer2.server
    ///implicit val config = Config.config
    ///implicit val ms: MetaStore = MetaStore
    lazy val connection = {
        ///System.setProperty(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://jobs-dev-sched2:9084")
        ///System.setProperty("hive.added.jars.path", "/Users/jeromebanks/hive-cdh421-patched/lib")
        ///System.setProperty("hive.aux.jars.path", "/Users/jeromebanks/hive-cdh421-patched/lib")
        ///System.setProperty("hive.exec.submitviachild", "true")
        DriverManager.getConnection(jdbcDriverUrl, "maxwell", "")
    }

    override def useDatabase(dbName: String) : Boolean = {
        println(" Using database " + dbName)
        val statement = connection.createStatement()
        statement.execute("use " + dbName)
    }

    override def executeQuery(query: String): Boolean = {
        try {
            val statement = connection.createStatement()
            statement.clearWarnings()
            statement.execute(query)
            if (statement.getWarnings() != null) {
                println(s" Warning !!! ${statement.getWarnings().getMessage}")
            }
            println(s" Query created  ${statement.getUpdateCount} rows")
            println(s" Max Rows = ${statement.getMaxRows} ")

            if (false) {
                val resultSet = statement.getResultSet()

                println(s" Query created  ${statement.getUpdateCount} rows")
                println(s" Max Rows = ${statement.getMaxRows} ")

                println("\n\n")
                var i = 0
                lazy val numCols = md.getColumnCount()
                for (i <- 1 to numCols) {
                    println(md.getColumnName(i) + "\t")
                }

                lazy val md = resultSet.getMetaData()
                while ({ resultSet.next() }) {
                    for (i <- 1 to numCols) {
                        val obj = resultSet.getObject(i)
                        println(obj.toString() + "\t")
                    }
                }
            }
            return true
        } catch {
            case sqlExc: SQLException =>
                println("Dammit !!! Caught SQLException " + sqlExc.getMessage())
                ///return false
                true

        }
    }
    
    def abort() {
       /// How to abort ???
       connection.rollback
    }

}