package hive.ms

import java.sql._
import org.apache.hive.jdbc._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.util.VersionInfo
import org.apache.hadoop.hive.ql.QueryPlan
import com.klout.satisfaction.MetricsProducing
import collection.JavaConversions._
import org.apache.hadoop.hive.ql.MapRedStats
import collection.mutable.{HashMap => MutableHashMap}
import com.klout.satisfaction.MetricsCollection

///import org.apache.hive.service.cli.HiveSQLException
//import org.apache.hadoop.hive.service.HiveServer
//import org.apache.hive.HiveServer2
/**
 * Executes jobs locally
 */
/**
 *  Trait for class which can executes
 *    Hive Queries,
 *     and interact with Hive ( i.e parse syntax, plan queries,
 *         define dependencies )
 *
 *   Can be both local, using Hive Driver implementation,
 *     or remote, using HiveServer2 JDBC client
 */
trait HiveDriver {

    def useDatabase(dbName: String);

    def executeQuery(query: String): Boolean;

}

class HiveLocalDriver extends HiveDriver with MetricsProducing {
    lazy implicit val hiveConf = Config.config

    lazy val driver = {
         /// XXX FIX ME ...
         ///   need to use project properties 
        hiveConf.set("mapreduce.framework.name", "classic")
        hiveConf.set("mapreduce.jobtracker.address", "jobs-dev-hnn:8021")
        hiveConf.set("mapred.job.tracker", "jobs-dev-hnn:8021")
        hiveConf.set("yarn.resourcemanager.address", "scr@wyoucloudera")

        ////hiveConf.set("hive.exec.post.hooks", "com.klout.satisfaction.GatherStatsHook")
        //// XXX TODO  Each project should have on set of auxjars 
        ///hiveConf.setAuxJars()
        val auxJars = auxJarsPath
        println(s" AUX JARS PATH = ${auxJars}")
        hiveConf.setAuxJars(auxJars)
        ///hiveConf.setAuxJars("/Users/jeromebanks/NewGit/satisfaction/auxlib")
        ///hiveConf.setAuxJars("/user/maxwell/hive_data_store/lib")

        println("Version :: " + VersionInfo.getBuildVersion)

        val dr = new org.apache.hadoop.hive.ql.Driver(hiveConf)

        dr.init
        SessionState.start(hiveConf)
        ///registerJars
        println(" HiveDriver is " + dr)
        println(" HiveConfig is " + hiveConf)
        val shims = ShimLoader.getHadoopShims
        println(" RPC port is " + shims.getJobLauncherRpcAddress(hiveConf))
        println(" Shims version is " + shims.getClass)
        dr
    }

    /// Want to make it on a per-project basis
    /// but for now, but them in the auxlib directory
    def registerJars(): Unit = {
        new java.io.File("/Users/jeromebanks/NewGit/satisfaction/auxlib").listFiles.filter(_.getName.endsWith("jar")).foreach(
            f => {
                println(s" Register jar ${f.getAbsolutePath} ")
                SessionState.registerJar("file:///" + f.getAbsolutePath)
            }
        )

    }

    def auxJarsPath(): String = {
      //// XXX associate aux lib with project 
      ////   link to project upload plugin ..
      ////   download from HDFS
        new java.io.File("/Users/jeromebanks/NewGit/satisfaction/auxlib").listFiles.filter(_.getName.endsWith("jar")).map(
            "file:////" + _.getAbsolutePath
        ).mkString(",")
    }

    override def useDatabase(dbName: String) = {
        ///val client = new HiveServer.HiveServerHandler
        println(" Using database " + dbName)
        executeQuery("use " + dbName)
        ///val client2 = new HiveServer2.HiveServerHandler
    }
    
    def getQueryPlan( query: String ) : QueryPlan = {
       val retCode = driver.compile(query)
       println(" Compiling " + query + " has return Code " + retCode)
       
      
       null
    }

    override def executeQuery(query: String): Boolean = {
        try {

            if (query.trim.startsWith("set") || query.trim.startsWith("SET")) {
                val setExpr = query.trim.split(" ")(1)
                val kv = setExpr.split("=")
                println(s" Setting configuration ${kv(0)} to ${kv(1)} ")
                SessionState.get.getConf.set(kv(0), kv(1))
                return true
            }

            val response = driver.run(query)
            if (response.getResponseCode() != 0) {
                println("Error while processing statement: " + response.getErrorMessage(), response.getSQLState(), response.getResponseCode());
                return false
            }

            true
        } catch {
            ///case sqlExc: HiveSQLException =>
            case sqlExc: Exception =>
                println("Dammit !!! Caught Hive SQLException " + sqlExc.getMessage())
                return false

        }
    }
    
    val SumCounters = List[String]()
        
   override def jobMetrics() : MetricsCollection = {
      
       val mc = new MetricsCollection("HiveQuery")
        updateJobMetrics( mc.metrics )
       mc
    }
    
  def updateJobMetrics( metricsMap : collection.mutable.Map[String,Any]) : Unit = {
      val mapRedStats : List[MapRedStats] = SessionState.get.getLastMapRedStatsList.toList
      var totalCpuMsec : Long = 0 
      var totalMappers : Long = 0
      var totalReducers :  Long = 0
      val counterSum : collection.mutable.Map[String,Long] = new MutableHashMap[String,Long]
      mapRedStats.foreach( mrs => {
          totalCpuMsec += mrs.getCpuMSec
          totalMappers += mrs.getNumMap
          totalReducers += mrs.getNumReduce
          
          println(" Examingng MapRedStats for job " + mrs.getJobId() )
          mrs.getCounters.write( new java.io.DataOutputStream(System.out))
          
          //// Printout counters 
      })
      
      metricsMap.put("TOTAL_NUM_MAPPERS", totalMappers)
      metricsMap.put("TOTAL_NUM_REDUCERS", totalReducers)
      metricsMap.put("TOTAL_CPU_MSEC", totalCpuMsec)
     
   }
   

}