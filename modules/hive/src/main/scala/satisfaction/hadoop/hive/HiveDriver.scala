package satisfaction
package hadoop
package hive

import scala.util.control.Breaks._
import org.apache.hive.jdbc._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.util.VersionInfo
import org.apache.hadoop.hive.ql.QueryPlan
import satisfaction.MetricsProducing
import collection.JavaConversions._
import org.apache.hadoop.hive.ql.MapRedStats
import collection.mutable.{HashMap => MutableHashMap}
import satisfaction.MetricsCollection
import scala.io.Source
import java.net.URLClassLoader
import java.io.File
import java.net.URL
import org.apache.hadoop.hive.ql.exec.Utilities
import java.lang.reflect.Method
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import java.io.BufferedReader
import java.io.FileReader
import scala.util.control.Breaks
import satisfaction.hadoop.Config
import org.apache.hadoop.hive.ql.HiveDriverRunHook
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext
import org.apache.hadoop.hive.ql.hooks.HookContext
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper

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

    def useDatabase(dbName: String) : Boolean 

    def executeQuery(query: String): Boolean
    
    def abort() 

}


/**
 *  Executes Jobs locally, going directly through 
 *    the internal 'SessionState' interface
 */

class HiveLocalDriver( val hiveConf : HiveConf = Config.config)
      extends HiveDriver with MetricsProducing with Logging {
  
    implicit var track : Track = null
  
    lazy val driver = {
      
        info("Version :: " + VersionInfo.getBuildVersion)

        val dr = new org.apache.hadoop.hive.ql.Driver(hiveConf)

        dr.init
        sessionState ///initials the Hive Session state

        
        val shims = ShimLoader.getHadoopShims
        info(" RPC port is " + shims.getJobLauncherRpcAddress(hiveConf))
        info(" Shims version is " + shims.getClass)
        dr
    }


    override def useDatabase(dbName: String) : Boolean = {
        info(" Using database " + dbName)
        executeQuery("use " + dbName)
    }
    
    def getQueryPlan( query: String ) : QueryPlan = {
       val retCode = driver.compile(query)
       info(" Compiling " + query + " has return Code " + retCode)
       
       
       driver.getPlan()
      
    }
    
    
    
    def sessionState : SessionState  = {
       var ss1 : SessionState = SessionState.get  
       if( ss1 == null) {
           ss1 = SessionState.start( hiveConf) 
           ss1.out = Console.out
           ss1.info = Console.out
       }
       ss1
    }
    
    def sourceFile( resourceName : String ) : Boolean = {
       info(s" Sourcing resource ## $resourceName ##")
       if( track.hasResource( resourceName ) ) {
         val readFile= track.getResource( resourceName) 
         println(s" ## Processing SourceFile ## $readFile")
        
          /// XXX Add variable substitution 
          ///readFile.split(";").forall( q => {
          readFile.split(";").foreach( q => {
           println(s" ## Executing sourced query $q") 
           executeQuery(q)
          } )
          true
       } else {
          warn(s"No resource $resourceName available to source.") 
          false
       }
    }
    
    
    def abort() = {
      
       /// Not sure this works with multiple Hive Goals ...
       /// Hive Driver is somewhat opaque
       info(" Aborting all jobs for Hive Query ")
       HadoopJobExecHelper.killRunningJobs()
      
    }

    override def executeQuery(query: String): Boolean = {
        try {

            info(s"HIVE_DRIVER :: Executing Query $query")
            println(s"HIVE_DRIVER :: Executing Query $query")
            if (query.trim.toLowerCase.startsWith("set")) {
                val setExpr = query.trim.split(" ")(1)
                val kv = setExpr.split("=")
                info(s" Setting configuration ${kv(0)} to ${kv(1)} ")
                sessionState.getConf.set(kv(0), kv(1))
                return true
            }
            if( query.trim.toLowerCase.startsWith("source") ) {
              val cmdArr = query.split(" ")
              if(cmdArr.size != 2 ) {
                warn(s" Unable to interpret source command $query ")
                return false 
              } else {
                 return sourceFile(cmdArr(1).replaceAll("'","").trim)
              }
            }

            sessionState.setIsVerbose(true)
            val response = driver.run(query)
            println(s"Response Code ${response.getResponseCode} :: SQLState ${response.getSQLState} ")
            if (response.getResponseCode() != 0) {
                println("Error while processing statement: " + response.getErrorMessage(), response.getSQLState(), response.getResponseCode());
                if(sessionState.getStackTraces != null)
                   sessionState.getStackTraces.foreach( { case( stackName , stackTrace) => {
                     println( s"## Stack $stackName ")
                     stackTrace.foreach { ln => println(s"      ##${ln}")  }
                    }
                  })
                return false
            } else {
            	readResults( response, 500)
            }

            true
        } catch {
            ///case sqlExc: HiveSQLException =>
            case sqlExc: Exception =>
                error("Dammit !!! Caught Hive SQLException " + sqlExc.getMessage())
                sqlExc.printStackTrace
                sqlExc.printStackTrace(System.out)
                sqlExc.printStackTrace(System.err)
                return false

        }
    }
    
    
    /// XXX FIX ME
    /// output is not being returned 
    def readResults( response : CommandProcessorResponse, maxRows : Int ) = {
      println(" Reading results")
      if(response.getSchema != null) {
        response.getSchema.getFieldSchemas.foreach( field => {
            print(s"${field.getName}\t")
        })
      }
      
      val tmpFile = sessionState.getTmpOutputFile
      val resultReader = new BufferedReader( new FileReader(tmpFile))
      breakable { for ( i<-0 to maxRows) {
         val line = resultReader.readLine
         if( line == null)  {
           break
         } else {
           println(line)
         }
        }
      }

    }
    
    val SumCounters = List[String]()
        
   override def jobMetrics() : MetricsCollection = {
      
       val mc = new MetricsCollection("HiveQuery")
        updateJobMetrics( mc.metrics )
       mc
    }
    
  def updateJobMetrics( metricsMap : collection.mutable.Map[String,Any]) : Unit = {
    val lastMapRedStats = sessionState.getLastMapRedStatsList
    if( lastMapRedStats != null) {
      val mapRedStats : List[MapRedStats] = lastMapRedStats.toList
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

}


object HiveDriver extends Logging {

  def apply(hiveConf: HiveConf): HiveDriver = {
    try {
      val parentLoader = if (Thread.currentThread.getContextClassLoader != null) {
        Thread.currentThread.getContextClassLoader
      } else {
        hiveConf.getClassLoader
      }
      val auxJars = hiveConf.getAuxJars
      if (auxJars != null) {
        val urls = auxJars.split(",").map(new URL(_))
        val urlClassLoader = new URLClassLoader(urls, parentLoader)
        debug(" URLS = " + urls.mkString(";"))
        hiveConf.setClassLoader(urlClassLoader)
      }

      val localDriverClass: Class[HiveLocalDriver] = hiveConf.getClass("com.klout.satisfaction.hadoop.hive.HiveLocalDriver", classOf[HiveLocalDriver]).asInstanceOf[Class[HiveLocalDriver]]
      val constructor = localDriverClass.getConstructor(hiveConf.getClass())
      val hiveDriver = constructor.newInstance(hiveConf).asInstanceOf[HiveLocalDriver]

      hiveDriver

    } catch {
      case e: Exception =>
        e.printStackTrace(System.out)
        ////error("Error while accessing HiveDriver", e)
        throw e
    }

  }
  
  val HiveCommentDelimiter = "---"

  /**
   *  Strip out comments starting with  ---,
   *  So that we can have comments in our Hive scripts ..
   *   #KillerApp
   */ 
  def stripComments( queryString : String ) : String = {
    queryString.split("\n").map( line => {
       if( line.contains(HiveCommentDelimiter) ) {
          line.substring( 0, line.indexOf(HiveCommentDelimiter)) 
       } else {
          line  
       }
     }
    ).mkString("\n")
  }

  
}