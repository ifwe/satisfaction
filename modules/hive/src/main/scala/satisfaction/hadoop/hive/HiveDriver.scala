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
import org.apache.hadoop.hive.ql.HiveDriverRunHook
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext
import org.apache.hadoop.hive.ql.hooks.HookContext
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper
import org.apache.hadoop.hive.ql.CommandNeedRetryException
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext

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

class HiveLocalDriver( val hiveConf : HiveConf = new HiveConf( Config.config))
      extends HiveDriver with MetricsProducing with Progressable with Logging {
  
    implicit var track : Track = null
    
    lazy val driver : org.apache.hadoop.hive.ql.Driver = {
        info("Version :: " + VersionInfo.getBuildVersion)

        /**
         *  Need to check our classloader
         */
        val dr = if( hiveConf.get("satisfaction.track.user.name") != null )  {
            new org.apache.hadoop.hive.ql.Driver(hiveConf, hiveConf.get("satisfaction.track.user.name"))
        } else {
           new org.apache.hadoop.hive.ql.Driver(hiveConf)
        }
        dr.init
        /**
        if(hiveConf.getVar( HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS ) != null )  {
          hiveConf.setVar( HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS , 
            hiveConf.getVar(HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS) + ",satifaction.hadoop.hive.HiveDriverHook" );
        } else {
          hiveConf.setVar( HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS , "satisfaction.hadoop.hive.HiveDriverHook");
        }
        * *
        */
        
        
        val shims = ShimLoader.getHadoopShims
        info(" RPC port is " + shims.getJobLauncherRpcAddress(hiveConf))
        info(" Shims version is " + shims.getClass)
        
        /**
         *     Set som
         */    
        ///Logger.getLogger( Configuration.class ).setLevel( INFO);
        
        
        
        dr
    }
    
    override lazy val progressCounter : ProgressCounter  = {
        new HiveProgress( this ) 
    }

    override def useDatabase(dbName: String) : Boolean = {
        info(" Using database " + dbName)
        executeQuery("use " + dbName)
    }
    
    def getQueryPlan( query: String ) : QueryPlan = {
       val retCode = driver.compile(query)
       info(s" Compiling $query  has return Code $retCode ")
       
       driver.getPlan()
    }
    
    def queryPlan : QueryPlan = {
      driver.getPlan()
    }
    
    
    def sessionState : SessionState  = {
       val ss1 : SessionState = SessionState.get  
       if( ss1 == null) {
          val  ss2 = SessionState.start( hiveConf) 
           ss2.out = Console.out
           ss2.info = Console.out
           ss2.childErr = Console.out
           ss2.childOut = Console.out
           ss2.err = Console.out
           ss2
       } else {
         ss1
       }
    }
    
    def sourceFile( resourceName : String ) : Boolean = {
       info(s" Sourcing resource ## $resourceName ##")
       if( track.hasResource( resourceName ) ) {
         val readFile= track.getResource( resourceName) 
           info(s" ## Processing SourceFile ## $readFile")
        
          /// XXX Add variable substitution 
          readFile.split(";").foreach( q => {
            if( q.trim.length > 0) {
               info(s" ## Executing sourced query $q") 
               if( !executeQuery(q) ) { 
                  return false
               }
            }
          } )
          true
       } else {
          warn(s"No resource $resourceName available to source.") 
          false
       }
    }
    
    
    override def abort() = {
       /// Not sure this works with multiple Hive Goals ...
       /// Hive Driver is somewhat opaque
       info("HIVE_DRIVER Aborting all jobs for Hive Query ")
       HadoopJobExecHelper.killRunningJobs()
    }
    
    
    /**
     *  Check that the  threadlocal SessionState
     *   has not been closed since last call
     */
    def checkSessionState() : Boolean = {
       val ss1 = SessionState.get()
       if(ss1 == null)  {
          val  ss2 = SessionState.start( hiveConf) 
           ss2.out = Console.out
           ss2.info = Console.out
           ss2.childErr = Console.out
           ss2.childOut = Console.out
           ss2.err = Console.out
           false
       } else {
           ss1.out = Console.out
           ss1.info = Console.out
           ss1.childErr = Console.out
           ss1.childOut = Console.out
           ss1.err = Console.out
         true 
       }
    }

    override def executeQuery(queryUnclean: String): Boolean = {
        try {
            val query : String = HiveLocalDriver.stripComments( queryUnclean)
            if(query.length == 0 ) {
              return true;
            }
            info(s"HIVE_DRIVER :: Executing Query $query")
            if (query.trim.toLowerCase.startsWith("set")) {
                val setExpr = query.trim.split(" ")(1)
                val kv = setExpr.split("=")
                //// add escaping ???
                info(s" Setting configuration ${kv(0).trim} to ${kv(1)} ")
                sessionState.getConf.set(kv(0).trim, kv(1))
                hiveConf.set(kv(0).trim, kv(1))
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
            sessionState.setConf( hiveConf)
            val response : CommandProcessorResponse = HiveLocalDriver.retry (5) {
                if( !checkSessionState ) {
                   warn(s"HIVE_DRIVER -- SessionState was closed after previous call ") 
                }
               val confMember = driver.getClass().getDeclaredField("conf") 
               confMember.setAccessible(true)
               
               val checkConf = confMember.get(driver).asInstanceOf[HiveConf]
            
            
                driver.run(query)
            }
            info(s"Response Code ${response.getResponseCode} :: SQLState ${response.getSQLState} ")
            if (response.getResponseCode() != 0) {
                error(s"HIVE_DRIVER Driver Has error Message ${driver.getErrorMsg()}")
                error(s"Error while processing statement: ${response.getErrorMessage()} ${response.getSQLState()} ${response.getResponseCode()}" );
                
                val driverClass = driver.getClass
                
                val errorMember =  driverClass.getDeclaredFields.filter( _.getName().endsWith("Error"))(0)
               
                errorMember.setAccessible(true)
                
                val errorStack : Throwable = errorMember.get( driver).asInstanceOf[Throwable]
                if( errorStack !=null) {
                   error(s"HIVE ERROR :: ERROR STACK IS $errorStack :: ${errorStack.getLocalizedMessage()} ")
                   if(errorStack.getCause != null) 
                      error(s"HIVE ERROR ::   CAUSE IS ${errorStack.getCause} :: ${errorStack.getCause.getLocalizedMessage()} ")
                } else {
                  error("HIVE ERROR :: ErrorStack is not set ") 
                }
                
                if(sessionState.getStackTraces != null) {
                   sessionState.getStackTraces.foreach { case( stackName , stackTrace) => {
                     error( s"## Stack $stackName ")
                     stackTrace.foreach { ln => error(s"      ##${ln}")  }
                    }
                  }
                }
                if(sessionState.getLocalMapRedErrors() != null) {
                	val localErrs = sessionState.getLocalMapRedErrors()
                	localErrs.foreach { case(key,errLines)  => {
                	    error(s"## LocalError  $key")
                	    errLines.foreach { ln => error(s"     ##${ln}") } 
                	 }
                	}
                }
                return false
            } else {
            	readResults( response, 500)
            }
            true
        } catch {
            ///case sqlExc: HiveSQLException =>
            case sqlExc: Exception  =>
                error(s"Dammit !!! Caught Hive SQLException ${sqlExc.getLocalizedMessage} ", sqlExc)
                return false
            case unexpected : Throwable => 
                error(s"Dammit !!! Unexpected SQLException ${unexpected.getLocalizedMessage} ", unexpected)
                throw unexpected
        }
    }
    
    
    /// XXX FIX ME
    /// output is not being returned 
    def readResults( response : CommandProcessorResponse, maxRows : Int ) = {
      if(response.getSchema != null) {
        response.getSchema.getFieldSchemas.foreach( field => {
            print(s"${field.getName}\t")
        })
      
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

  def apply(hiveConf: HiveConf)(implicit track : Track): HiveDriver = {
    try {
      val parentLoader = if (Thread.currentThread.getContextClassLoader != null) {
        Thread.currentThread.getContextClassLoader
      } else {
        hiveConf.getClassLoader
      }
      val auxJars = hiveConf.getAuxJars
     
      info( s" Track libPath is ${track.libPath}")
      val urls = track.hdfs.listFiles( track.libPath).map( _.path.toUri.toURL)
      val urlClassLoader = URLClassLoader.newInstance( urls.toArray[URL], parentLoader);
      hiveConf.setClassLoader( urlClassLoader)
      val auxJarPath = track.hdfs.listFiles( track.libPath).map( _.path.toUri.toString ).mkString(",")
      info(" Using AuxJarPath " + auxJarPath)
      hiveConf.setAuxJars( auxJarPath)
      hiveConf.set("hive.aux.jars.path", auxJarPath)
      //// XXX Move to Scala reflection ...
      val localDriverClass: Class[HiveLocalDriver] = hiveConf.getClass("satisfaction.hadoop.hive.HiveLocalDriver", classOf[HiveLocalDriver]).asInstanceOf[Class[HiveLocalDriver]]
      val constructor = localDriverClass.getConstructor(hiveConf.getClass())
      val hiveDriver = constructor.newInstance(hiveConf).asInstanceOf[HiveLocalDriver]
      hiveDriver.track = track

      hiveDriver

    } catch {
      case e: Exception =>
        e.printStackTrace(System.out)
        ////error("Error while accessing HiveDriver", e)
        throw e
    }

  }
  
}

class HiveDriverHook extends HiveDriverRunHook with Logging {
     /**
   * Invoked before Hive begins any processing of a command in the Driver,
   * notably before compilation and any customizable performance logging.
   */
   def preDriverRun(hookContext : HiveDriverRunHookContext)  = {
     
      info("HIVE_DRIVER :: PRE DRIVER RUN :: " + hookContext.getCommand())
      SessionState.getConsole.printInfo("HIVE_DRIVER :: PRE DRIVER RUN :: " + hookContext.getCommand())
   }

  /**
   * Invoked after Hive performs any processing of a command, just before a
   * response is returned to the entity calling the Driver.
   */
   def postDriverRun( hookContext : HiveDriverRunHookContext) = {
     info(" HIVE DRIVER POST RUN " + hookContext.getCommand() )
     SessionState.get.getLastMapRedStatsList()
     SessionState.getConsole().printInfo("HIVE DRVER POST RUN " + hookContext.getCommand() )
   }
    
}
  
object HiveLocalDriver {
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
    ).mkString("\n").trim
  }
  

  /**
   * Logic for retrying a command 
   */
  def retry[T <: AnyRef]( numRetries: Int = 3)( f: => T) : T = {
    var cnt = 0;
     while( true ) {
      try {
         return f
      }  catch {
        case  retry : CommandNeedRetryException => {
           println(s" Number of retries = $cnt")
           cnt += 1
           if( cnt == numRetries) {
             if( retry.getCause != null)
                throw retry.getCause
              else 
                throw retry
           }
        } 
        case unexpected : Throwable => throw unexpected
      }
    }
    null.asInstanceOf[T]
  }
}
