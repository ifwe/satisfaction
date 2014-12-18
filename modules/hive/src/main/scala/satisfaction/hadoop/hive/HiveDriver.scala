package satisfaction
package hadoop.hive 

import satisfaction.Logging
import scala.util.control.Breaks._
import _root_.org.apache.hive.jdbc._
import _root_.org.apache.hadoop.hive.conf.HiveConf
import _root_.org.apache.hadoop.hive.ql.session.SessionState
import _root_.org.apache.hadoop.hive.shims.ShimLoader
import _root_.org.apache.hadoop.util.VersionInfo
import _root_.org.apache.hadoop.hive.ql.QueryPlan
import satisfaction.MetricsProducing
import collection.JavaConversions._
import _root_.org.apache.hadoop.hive.ql.MapRedStats
import collection.mutable.{HashMap => MutableHashMap}
import satisfaction.MetricsCollection
import satisfaction.hadoop.Config
import scala.io.Source
import java.net.URLClassLoader
import java.io.File
import java.net.URL
import _root_.org.apache.hadoop.hive.ql.exec.Utilities
import java.lang.reflect.Method
import _root_.org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import java.io.BufferedReader
import java.io.FileReader
import scala.util.control.Breaks
import _root_.org.apache.hadoop.hive.ql.HiveDriverRunHook
import _root_.org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext
import _root_.org.apache.hadoop.hive.ql.hooks.HookContext
import _root_.org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper
import _root_.org.apache.hadoop.hive.ql.CommandNeedRetryException
import _root_.org.apache.hadoop.hive.ql.HiveDriverRunHookContext
import satisfaction.Track
import satisfaction.Logging
import satisfaction.Progressable
import satisfaction.ProgressCounter
import _root_.org.apache.commons.logging.Log
import _root_.org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import _root_.org.apache.hadoop.hive.metastore.HiveMetaHookLoader
import satisfaction.hadoop.CachingTrackLoader
import _root_.org.apache.hadoop.hive.ql.metadata.Hive

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
    
    def setProperty( prop : String, propValue : String )
    
    def abort() 
    
    def close() 

}


/**
 *  Executes Jobs locally, going directly through 
 *    the internal 'SessionState' interface
 */

class HiveLocalDriver( val hiveConf : HiveConf = new HiveConf( Config.config ))
      extends satisfaction.hadoop.hive.HiveDriver with MetricsProducing with Progressable with Logging {
 
            
    /// Set up a new Hive on this thread, with our HiveConf
    val newHive  = Hive.set( Hive.get(hiveConf,true))
    
    ///lazy val driver : _root_.org.apache.hadoop.hive.ql.Driver = {
    def getDriver : _root_.org.apache.hadoop.hive.ql.Driver = {

        
        val cl = this.getClass.getClassLoader
        info( " HiveLocalDriver getDriver  ClassLoader = " + this.getClass.getClassLoader.getClass().getName() )
        info( " HiveLocalDriver getDriver  ThreadLoader= = " + Thread.currentThread().getContextClassLoader().getClass().getName() )
        ////Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
        
        info(s"  SessionState Classloader =  ${classOf[SessionState].getClassLoader}  THIS LOADER = $cl " )
        /**
         *  Need to check our classloader
         */
        /**
        val dr = if( hiveConf.get("satisfaction.track.user.name") != null )  {
            new _root_.org.apache.hadoop.hive.ql.Driver(hiveConf, hiveConf.get("satisfaction.track.user.name"))
        } else {
           new _root_.org.apache.hadoop.hive.ql.Driver(hiveConf)
        }
        * 
        */
        /**
        val  apacheHiveDriverClass = cl.loadClass("org.apache.hadoop.hive.ql.Driver")
        val dr = apacheHiveDriverClass.getConstructor( classOf[HiveConf]).newInstance( hiveConf).asInstanceOf[_root_.org.apache.hadoop.hive.ql.Driver]
        * 
        */
        //// implicitly use same classloader as this 
        try {
          val dr = new _root_.org.apache.hadoop.hive.ql.Driver( hiveConf)
          info( s" New APACHE DRIVER = $dr  CLASS LOADER = ${dr.getClass.getClassLoader}" )
        
        
          dr.init
           dr
        
        }  catch {
          case unexpected : Throwable => {
            error(s"Unexpected error while creating HiveDriver ${unexpected.getMessage()} ", unexpected )
            throw unexpected
          } 
        }
        
    }
    
    override def close() = {
      val thisClassLoader = this.getClass().getClassLoader
      thisClassLoader match {
        case closable : java.io.Closeable => {
           info(s" Closing Closable ClassLoader $thisClassLoader ")      
           closable.close
        }
        case _ => {
           info(" Our classloader was not closable") 
        }
      }
    }
    
    
    override def setProperty( prop : String , propValue : String) = {
      this.hiveConf.set( prop, propValue)
    }
    
    override lazy val progressCounter : ProgressCounter  = {
        new HiveProgress( this ) 
    }

    override def useDatabase(dbName: String) : Boolean = {
        info(" Using database " + dbName)
        executeQuery("use " + dbName)
    }
    
    def getQueryPlan( query: String ) : QueryPlan = {
       val driver = getDriver
       val retCode = driver.compile(query)
       info(s" Compiling $query  has return Code $retCode ")
       
       driver.getPlan()
    }
    
    
    private var _sessionState :SessionState = null
    lazy val sessionState: SessionState  = {
       info(s" Starting SessionState !!!")
       if( _sessionState == null) {
       val ss1 : SessionState = SessionState.start( hiveConf)
       ss1.out = Console.out
       ss1.info = Console.out
       ss1.childErr = Console.out
       ss1.childOut = Console.out
       ss1.err = Console.out
       ss1.setIsVerbose(true)
       _sessionState = ss1
        
       }
       _sessionState
    }
    
    
    override def abort() = {
       /// Not sure this works with multiple Hive Goals ...
       /// Hive Driver is somewhat opaque
       info("HIVE_DRIVER Aborting all jobs for Hive Query ")
       HadoopJobExecHelper.killRunningJobs()
       ///// driver.destroy
    }
    
    
    /**
     *  Check that the  threadlocal SessionState
     *   has not been closed since last call
     */
    /**
    def checkSessionState() : Boolean = {
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
    * 
    */

    override def executeQuery(query: String): Boolean = {
        try {

            var driver  : _root_.org.apache.hadoop.hive.ql.Driver = null
            val response : CommandProcessorResponse = HiveLocalDriver.retry (5) {
              /**
                if( !checkSessionState ) {
                   warn(s"HIVE_DRIVER -- SessionState was closed after previous call ") 
                }
                * 
               val confMember = driver.getClass().getDeclaredField("conf") 
               confMember.setAccessible(true)
               
               val checkConf = confMember.get(driver).asInstanceOf[HiveConf]
                */
               if(driver != null) {
                 driver.close()
               }
            
            
                driver = getDriver
            	SessionState.setCurrentSessionState( sessionState )
            	val cl = Thread.currentThread().getContextClassLoader();
                if( cl != hiveConf.getClassLoader() ) {
                  error(s" ERROR SOMEONE OVERWROTE CLASS LOADER IN THREAD CONTEXT  $cl ${hiveConf.getClassLoader}" )
                  error(s" ERROR SOMEONE OVERWROTE CLASS LOADER IN THREAD CONTEXT Context =  $cl HiveConf = ${hiveConf.getClassLoader} this loader = ${this.getClass.getClassLoader} " )
                  Console.out.println(s" ERROR SOMEONE OVERWROTE CLASS LOADER IN THREAD CONTEXT Context =  $cl HiveConf = ${hiveConf.getClassLoader} this loader = ${this.getClass.getClassLoader} " )
                  System.out.println(s" ERROR SOMEONE OVERWROTE CLASS LOADER IN THREAD CONTEXT  Context = $cl  HiveConf =  ${hiveConf.getClassLoader} this loader = ${this.getClass.getClassLoader}" )
                }
                ///Thread.currentThread.setContextClassLoader( hiveConf.getClassLoader )
                val resp = driver.run(query)
                driver.close()
                driver.destroy() 
                resp
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
    if(_sessionState != null) {
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
          
          println(" Examining MapRedStats for job " + mrs.getJobId() )
          mrs.getCounters.write( new java.io.DataOutputStream(System.out))
          
          //// Printout counters 
      })
      
      metricsMap.put("TOTAL_NUM_MAPPERS", totalMappers)
      metricsMap.put("TOTAL_NUM_REDUCERS", totalReducers)
      metricsMap.put("TOTAL_CPU_MSEC", totalCpuMsec)
    }
    }
     
   }

}    


object HiveDriver extends Logging {

  def apply(hiveConf: HiveConf)(implicit track : Track): HiveDriver = {
    try {
      info( s" ThreadLoader = ${Thread.currentThread.getContextClassLoader}  HiveConfLoader = ${hiveConf.getClassLoader} This loader = ${this.getClass.getClassLoader} ")
      val parentLoader = if (Thread.currentThread.getContextClassLoader != null) {
        Thread.currentThread.getContextClassLoader
      } else {
        hiveConf.getClassLoader
      }
      info(s" ParentLoader = ${parentLoader} ")
      val auxJars = hiveConf.getAuxJars
     
      info( s" Track libPath is ${track.libPath}")
      info( s" Track resourcePath is ${track.resourcePath}")
      ///val urls = track.hdfs.synchronized { track.hdfs.listFiles( track.libPath) }
      ///val resources = track.hdfs.synchronized { track.hdfs.listFiles( track.resourcePath) }
      ///val exportFiles = ( urls ++ resources)
      ///val urlClassLoader = new harmony.java.net.ReverseClassLoader( exportFiles.map( _.path.toUri.toURL).toArray[URL], parentLoader);
      //val urls =  track.hdfs.listFiles( track.libPath) 
      val urls =  track.listLibraries 
      //val resources =  track.hdfs.listFiles( track.resourcePath) 
      val resources =  track.listResources
      val exportFiles = ( urls ++ resources)
      
      val isolateFlag = track.trackProperties.getProperty("satisfaction.classloader.isolate","true").toBoolean
      val urlClassLoader = if( isolateFlag) {
         val cachePath = CachingTrackLoader.getCachePath( track.trackPath ).toString
         info(s" Using IsolatedClassLoader with a cachePath of $cachePath")
         
         val frontLoadClasses =  List("org.apache.hadoop.hive.ql*", 
    		  "satisfaction.hadoop.hive.HiveLocalDriver", 
    		  "satisfaction.hadoop.hive.HiveLocalDriver.*", 
    		  "satisfaction.hadoop.hive.*", 
    		  "satisfaction.hadoop.hdfs.*",
    		  "brickhouse.*",
    		  "com.tagged.udf.*",
    		  "com.tagged.hadoop.hive.*")
         val backLoadClasses = List(
                  "satisfaction.hadoop.hive.HiveSatisfier",
                  "org.apache.hadoop.hive.conf.*",
    		      "org.apache.hive.common.*",
    		      "org.apache.hadoop.hive.common.*",
                  "org.apache.commons.logging.*",
                  "org.apache.hadoop.hive.ql.metadata.*",
                  "org.apache.hadoop.hive.metastore.*"
                  ////"org.apache.*HiveMetaStoreClient.*",
                  ///"org.apache.*IMetaStoreClient.*",
                  ////"org.apache.hadoop.hive.metastore.*",
                  ///"org.apache.hadoop.hive.ql.lockmgr.*",
                  ////"org.apache.hadoop.hive.metastore.api.*",
                  ///"org.apache.*HiveMetaHookLoader.*")
                  )
         val isolatedClassLoader = new harmony.java.net.IsolatedClassLoader( exportFiles.map( _.toUri.toURL).toArray[URL], 
    		  	parentLoader,
    		  	frontLoadClasses,
    		  	backLoadClasses, 
    		  	hiveConf,
    		  	cachePath);
         isolatedClassLoader.registerClass(classOf[HiveDriver]);
         ///isolatedClassLoader.registerClass(classOf[com.tagged.hadoop.hive.serde2.avro.AvroSerDe]);
         ///isolatedClassLoader.registerClass(classOf[HiveConf]);
         ////isolatedClassLoader.registerClass(classOf[HiveMetaStoreClient]);
         /////isolatedClassLoader.registerClass(classOf[HiveMetaHookLoader]);
         
         ////isolatedClassLoader.registerClass(classOf[Log]);
         hiveConf.setVar(HiveConf.ConfVars.HIVE_PERF_LOGGER, "satisfaction.hadoop.hive.BoogerPerfLogger");
         info( s" LOG CLASSLOADER is ${classOf[Log].getClassLoader}")
          if( track.trackProperties.contains("satisfaction.classloader.frontload"))  {
              track.trackProperties.getProperty("satisfaction.classloader.frontload").split(",").foreach( expr => {
                 isolatedClassLoader.addFrontLoadExpr( expr);
              })
          }
          if( track.trackProperties.contains("satisfaction.classloader.backload"))  {
             track.trackProperties.getProperty("satisfaction.classloader.backload").split(",").foreach( expr => {
                isolatedClassLoader.addFrontLoadExpr( expr);
             })
         }
         isolatedClassLoader
     } else {
          ///java.net.URLClassLoader.newInstance( exportFiles.map( _.toUri.toURL).toArray[URL], 
    		  	///parentLoader )
          java.net.URLClassLoader.newInstance( exportFiles.map( _.toUri.toURL).toArray[URL] )
      }
      
      ////val  checkMetaStoreUtils = classOf[MetaStoreUtils].getDeclaredMethods.filter( _.get )
      
      
      
      

      ///urlClassLoader.setLogger( log)
      ///urlClassLoader.setName( track.descriptor.trackName)

      hiveConf.setClassLoader( urlClassLoader);
      Thread.currentThread().setContextClassLoader(urlClassLoader)

      val auxJarPath = exportFiles.map( _.toUri.toString ).mkString(",")
      
      info(" Using AuxJarPath " + auxJarPath)
      hiveConf.setAuxJars( auxJarPath)
      hiveConf.set("hive.aux.jars.path", auxJarPath)
      //// XXX Move to Scala reflection ...
      info( "Instantiating HiveLocalDriver")
      val localDriverClass: Class[_] = urlClassLoader.loadClass("satisfaction.hadoop.hive.HiveLocalDriver")
      info( s" Local Driver Class is $localDriverClass ")
      val constructor = localDriverClass.getConstructor(hiveConf.getClass())
      val satisfactionHiveConf = new SatisfactionHiveConf(hiveConf)
      satisfactionHiveConf.setClassLoader( urlClassLoader)
      
      val newHive  = Hive.set( Hive.get( satisfactionHiveConf,true))

      val hiveLocalDriver = constructor.newInstance(satisfactionHiveConf)
      info( s" Hive Local Driver is ${hiveLocalDriver} ${hiveLocalDriver.getClass} ")

      
      hiveLocalDriver match {
        case traitDriver : HiveDriver => {
            info(s" Local Driver $hiveLocalDriver is Trait Driver $traitDriver" )
            return traitDriver
        }
        case _ => {
          error(s" LocalDriver $hiveLocalDriver really isn't a Hive Driver !!!!")
          warn(s" LocalDriver $hiveLocalDriver really isn't a Hive Driver !!!!")
          error( s" HiveDriver Class is  ${classOf[HiveDriver]} ${classOf[HiveDriver].hashCode()} Loader is ${classOf[HiveDriver].getClassLoader} ")
          error( " TRAITS of localDriver ")
          localDriverClass.getInterfaces().foreach( ifc => {
               error( s" TRAIT CLASS ${ifc} ${ifc.getCanonicalName} ${ifc.hashCode} ${ifc.getClassLoader} ") 
          })
          throw new RuntimeException(s" LocalDriver $hiveLocalDriver really isn't a Hive Driver !!!!")
        }
      }

    } catch {
      case e: Exception =>
        e.printStackTrace(System.out)
        error("Error while accessing HiveDriver", e)
        throw e
    }

  }
  
}


class SatisfactionHiveConf(hc : HiveConf) extends HiveConf(hc) with Logging {
  
  /**
   *   Don't Cache !!!
   */
  override def getClassByName( className : String ) : Class[_] = {
      info(s" Loading HiveConf class $className with ClassLoader ${getClassLoader}" ) 
      
      getClassLoader.loadClass(className)
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
        case unexpected : Throwable => {
          throw unexpected
        }
      }
    }
    null.asInstanceOf[T]
  }
}
