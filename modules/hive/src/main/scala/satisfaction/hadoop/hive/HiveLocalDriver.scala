package satisfaction
package hadoop.hive 

import java.io.BufferedReader
import java.io.FileReader
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import _root_.org.apache.hadoop.hive.conf.HiveConf
import _root_.org.apache.hadoop.hive.ql.CommandNeedRetryException
import _root_. org.apache.hadoop.hive.ql.MapRedStats
import _root_.org.apache.hadoop.hive.ql.QueryPlan
import _root_.org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper
import _root_.org.apache.hadoop.hive.ql.metadata.Hive
import _root_.org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import _root_.org.apache.hadoop.hive.ql.session.SessionState
import satisfaction.Logging
import satisfaction.MetricsCollection
import satisfaction.MetricsProducing
import satisfaction.ProgressCounter
import satisfaction.Progressable
import satisfaction.hadoop.Config


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
        
        ///info(s"  SessionState Classloader =  ${classOf[SessionState].getClassLoader}  THIS LOADER = $cl " )
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
        try {
        val  apacheHiveDriverClass = cl.loadClass("org.apache.hadoop.hive.ql.Driver")
        val dr = apacheHiveDriverClass.getConstructor( classOf[HiveConf]).newInstance( hiveConf).asInstanceOf[_root_.org.apache.hadoop.hive.ql.Driver]
        //// implicitly use same classloader as this 
          ///val dr = new _root_.org.apache.hadoop.hive.ql.Driver( hiveConf)
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
