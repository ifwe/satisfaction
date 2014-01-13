package com.klout
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
import com.klout.satisfaction.MetricsProducing
import collection.JavaConversions._
import org.apache.hadoop.hive.ql.MapRedStats
import collection.mutable.{HashMap => MutableHashMap}
import com.klout.satisfaction.MetricsCollection
import scala.io.Source
import java.net.URLClassLoader
import java.io.File
import java.net.URL
import org.apache.hadoop.hive.ql.exec.Utilities
import java.lang.reflect.Method
import com.klout.satisfaction.TrackOriented
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import java.io.BufferedReader
import java.io.FileReader
import scala.util.control.Breaks

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



class HiveLocalDriver extends HiveDriver with MetricsProducing with TrackOriented {
    lazy implicit val hiveConf = Config.config

    lazy val driver = {
      
      
        ////hiveConf.set("hive.exec.post.hooks", "com.klout.satisfaction.GatherStatsHook")
        //// XXX TODO  Each project should have on set of auxjars 
        ///hiveConf.setAuxJars()
        val auxJars = auxJarsPath
        println(s" AUX JARS PATH = ${auxJars}")
        hiveConf.setAuxJars(auxJars)

        println("Version :: " + VersionInfo.getBuildVersion)

        val dr = new org.apache.hadoop.hive.ql.Driver(hiveConf)

        dr.init
        SessionState.start(hiveConf)
        registerJars
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
        auxJarFolder.listFiles.filter(_.getName.endsWith("jar")).foreach(
            f => {
                println(s" Register jar ${f.getAbsolutePath} ")
                val jarUrl = "file://" + f.getAbsolutePath
                SessionState.registerJar(jarUrl)
                
            }
        )
    }
    
    def auxJarFolder : File = {
       if(_auxJarFolder != null)  {
         new File(_auxJarFolder)
       } else {
         track.auxJarFolder
       }
    }
    
    private var _auxJarFolder : String = null

    def setAuxJarFolder( folder : String) = {
      _auxJarFolder = folder 
    }
    
    def auxJarsPath: String = {
      //// XXX associate aux lib with project 
      ////   link to project upload plugin ..
      ////   download from HDFS
      auxJarFolder.listFiles.filter(_.getName.endsWith("jar")).map(
      ////new File("/Users/jeromebanks/NewGit/satisfaction/apps/willrogers/lib").listFiles.filter(_.getName.endsWith("jar")).map(
            "file://" + _.getAbsolutePath
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
       
       
       driver.getPlan()
      
    }
    
    def sourceFile( fileName : String ) : Boolean = {
       println(s" Sourcing file $fileName")
       val readFile   =scala.io.Source.fromFile( fileName ).mkString
       println(s" Text is $readFile")
       //// XXX do proper escaping, and parse out comments ...
       readFile.split(";").filter( _.startsWith("---")).forall( executeQuery(_) )
    }

    override def executeQuery(query: String): Boolean = {
        try {

            println(s"HIVE_DRIVER :: Executing Query $query")
            if (query.trim.toLowerCase.startsWith("set")) {
                val setExpr = query.trim.split(" ")(1)
                val kv = setExpr.split("=")
                println(s" Setting configuration ${kv(0)} to ${kv(1)} ")
                if( SessionState.get == null)
                  SessionState.start( hiveConf)
                SessionState.get.getConf.set(kv(0), kv(1))
                return true
            }
            if( query.trim.toLowerCase.equals("source") || query.contains("oozie-setup")) {
              /// XXX TODO source the file ...
              /// for now ignore, because always oozie-setup.hql 
              println(s" Ignoring source statement $query ")
              
              return true
            }

            val response = driver.run(query)
            println(s"Response Code ${response.getResponseCode} :: SQLState ${response.getSQLState} ")
            if (response.getResponseCode() != 0) {
                println("Error while processing statement: " + response.getErrorMessage(), response.getSQLState(), response.getResponseCode());
                return false
            } else {
            	readResults( response, 500)
            }

            true
        } catch {
            ///case sqlExc: HiveSQLException =>
            case sqlExc: Exception =>
                println("Dammit !!! Caught Hive SQLException " + sqlExc.getMessage())
                sqlExc.printStackTrace
                return false

        }
    }
    
    
    def readResults( response : CommandProcessorResponse, maxRows : Int ) = {
      if(response.getSchema != null) {
        response.getSchema.getFieldSchemas.foreach( field => {
            print(s"${field.getName}\t")
        })
      }
      val session = SessionState.get
      
      val tmpFile = session.getTmpOutputFile
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
    val lastMapRedStats = SessionState.get.getLastMapRedStatsList
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


object HiveDriver {
 
   def apply( auxJarPath : String ) :HiveDriver = {
     
     val parentLoader = if( Thread.currentThread.getContextClassLoader != null) { 
       Thread.currentThread.getContextClassLoader
     } else { 
        this.getClass.getClassLoader
     }
     val pathFile = new File( auxJarPath)
     val urls = pathFile.listFiles.map("file://" + _.getPath ).map( new URL(_))
     val urlClassLoader = new URLClassLoader( urls, parentLoader)
     Thread.currentThread.setContextClassLoader( urlClassLoader)
     
     val driverClass = urlClassLoader.loadClass("hive.ms.HiveLocalDriver")
     
     val hiveDriver = driverClass.newInstance.asInstanceOf[HiveLocalDriver]
     val method = driverClass.getMethod( "setAuxJarFolder", classOf[String] )
     method.invoke(hiveDriver, auxJarPath)
     
    
     
     
     hiveDriver
   }
  
  
}