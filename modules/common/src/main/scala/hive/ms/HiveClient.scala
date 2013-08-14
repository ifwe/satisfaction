package hive.ms

import java.sql._
/**
 * Executes jobs locally
 */
class HiveClient(val jdbcDriverUrl: String) {
    val driverClass = Class.forName("org.apache.hive.jdbc.HiveDriver")
    val connection = DriverManager.getConnection(jdbcDriverUrl)

    def useDatabase(dbName: String) {
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