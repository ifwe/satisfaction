package hive.ms

import java.sql._
import org.apache.hive.jdbc._
/**
 * Executes jobs locally
 */
class HiveClient(val jdbcDriverUrl: String) {
    val driverClass = Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver")
    val connection = DriverManager.getConnection(jdbcDriverUrl)

    def useDatabase(dbName: String) {
        println(" Using database " + dbName)
        val statement = connection.createStatement()
        statement.execute("use " + dbName)
    }

    def executeQuery(query: String) {
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)

        /// TODO .. Actually return rows ???
    }

}
//// Use the embedded HiveClient by default
object HiveClient extends HiveClient("jdbc:hive2://") {

}