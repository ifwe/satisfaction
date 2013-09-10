package hive.ms

import java.sql._
import org.apache.hive.jdbc._
import org.apache.hadoop.hive.conf.HiveConf
//import org.apache.hadoop.hive.service.HiveServer
//import org.apache.hive.HiveServer2
/**
 * Executes jobs locally
 */
class HiveClient(val jdbcDriverUrl: String) {
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

    def useDatabase(dbName: String) {
        ///val client = new HiveServer.HiveServerHandler
        println(" Using database " + dbName)
        val statement = connection.createStatement()
        statement.execute("use " + dbName)
        ///val client2 = new HiveServer2.HiveServerHandler
    }

    def executeQuery(query: String): Boolean = {
        try {
            val statement = connection.createStatement()
            statement.clearWarnings()
            val resultSet = statement.executeQuery(query)
            if (statement.getWarnings() != null) {
                println(s" Warning !!! ${statement.getWarnings().getMessage}")

            }

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
            return true
        } catch {
            case sqlExc: SQLException =>
                println("Dammit !!! Caught SQLException " + sqlExc.getMessage())
                return false

        }
    }

}
//// Use the embedded HiveClient by default
////object HiveClient extends HiveClient("jdbc:hive2://") {
object HiveClient extends HiveClient("jdbc:hive2://jobs-dev-sched2:11112") {

    ///object HiveClient extends HiveClient("jdbc:hive2://localhost:1111") {

}