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
import satisfaction.Logging
import satisfaction.MetricsCollection
import satisfaction.MetricsProducing
import satisfaction.ProgressCounter
import satisfaction.Progressable
import satisfaction.hadoop.Config
import java.io.File
import satisfaction.util.classloader.IsolatedClassLoader
import satisfaction.util.Releaseable
import satisfaction.util.Wrapper


/**
 *  Executes Jobs locally, going directly through 
 *    the internal 'SessionState' interface
 */

class HiveLocalDriver( val hiveConf : HiveConf = new HiveConf( Config.config ) )
      ///extends satisfaction.hadoop.hive.HiveDriver with MetricsProducing with Progressable with Logging {
      extends satisfaction.hadoop.hive.HiveDriver with MetricsProducing  {
  
  
    /// To avoid linkage errors for now ...
    def info(ms : String ) = { println(s" INFO HiveLocalDriver ::  $ms ") }
    def error(ms : String ) = 
    { println(s" ERROR HiveLocalDriver ::  $ms ") }
    def error(ms : String, unexpect : Throwable ) = {
      println(s" ERROR HiveLocalDriver ::  $ms ")
      unexpect.printStackTrace
    }
 
     val driver = new Releaseable[Wrapper]({

        
        val cl = this.getClass.getClassLoader() 
        info( " HiveLocalDriver getDriver  Cl = " + cl )
        info( " HiveLocalDriver getDriver  ClassLoader = " + this.getClass.getClassLoader.getClass().getName() )
        info( " HiveLocalDriver getDriver  ThreadLoader= = " + Thread.currentThread().getContextClassLoader().getClass().getName() )
        try {
          
          val dr = Wrapper.withConstructor( "org.apache.hadoop.hive.ql.Driver", cl, Array[Class[_]]( classOf[HiveConf] ) , Array(  hiveConf ) )
          info( s" New APACHE DRIVER = ${dr.wrapped}  CLASS LOADER = ${dr.wrapped.getClass.getClassLoader}" )
        
        
          dr.->("init")
           dr
        
        }  catch {
          case unexpected : Throwable => {
            error(s"Unexpected error while creating HiveDriver ${unexpected.getMessage()} ", unexpected )
            throw unexpected
          } 
        }
        
    })
    
    override def close() = {
       driver.get.->("close")
       driver.release
      val thisClassLoader = this.getClass().getClassLoader
      thisClassLoader match {
        case closable : java.io.Closeable => {
           info(s" HiveLocalDriver :: Closing Closable ClassLoader $thisClassLoader ")      
           closable.close
        }
        case _ => {
           info(" HiveLocalDriver :: Our classloader was not closable") 
        }
      }
    }
    
    
    override def setProperty( prop : String , propValue : String) = {
      this.hiveConf.set( prop, propValue)
    }
    
    /**
    override lazy val progressCounter : ProgressCounter  = {
        new HiveProgress( this ) 
    }
    * 
    */

    override def useDatabase(dbName: String) : Boolean = {
        info(" Using database " + dbName)
        executeQuery("use " + dbName)
    }
    
    def getQueryPlan( query: String ) : QueryPlan = {
       val retCode = driver.get.->("compile",query)
       info(s" Compiling $query  has return Code $retCode ")
       
       driver.get.->("getPlan").asInstanceOf[QueryPlan]
    }
    
    
    private var _sessionState : Wrapper = null
    ////lazy val sessionState: SessionState  = {
    lazy val sessionState: Wrapper  = {
       info(s" Starting SessionState !!!")
       if( _sessionState == null) {
       val ss1 : Wrapper  = new Wrapper( Wrapper.execStatic( "org.apache.hadoop.hive.ql.session.SessionState", this.getClass.getClassLoader, "start",  new HiveConf(hiveConf)) )
       ss1 ##= ( "out", Console.out)
       ss1 ##= ( "info" , Console.out )
       ss1 ##= ( "childErr" , Console.out )
       ss1 ##= ( "childOut" , Console.out )
       ss1 ##= ( "err" , Console.out )
       ss1.execWithParams( "setIsVerbose",Array[Class[_]]( java.lang.Boolean.TYPE ), Array( new java.lang.Boolean(true)))
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
       close
    }
    
    
    override def executeQuery(query: String): Boolean = {
        try {
            val response : CommandProcessorResponse = HiveLocalDriver.retry (5) {
                sessionState.execStatic("setCurrentSessionState", sessionState.wrapped)
                info( s" SESSION STATE CL = ${sessionState.wrapped} ${sessionState.wrappedClass.getClassLoader} ")

                val resp = driver.get.->("run",query).asInstanceOf[CommandProcessorResponse]
                resp
            }

            info(s"Response Code ${response.getResponseCode} :: SQLState ${response.getSQLState} ")
            if (response.getResponseCode() != 0) {
                error(s"HIVE_DRIVER Driver Has error Message ${driver.->("getErrorMsg")}")
                error(s"Error while processing statement: ${response.getErrorMessage()} ${response.getSQLState()} ${response.getResponseCode()}" );
                
                val driverClass = driver.get.wrapped.getClass
                
                /// XXX Use wrapper class ...
                val errorMember =  driverClass.getDeclaredFields.filter( _.getName().endsWith("Error"))(0)
               
                errorMember.setAccessible(true)
                
                val errorStack : Throwable = errorMember.get( driver.get.wrapped).asInstanceOf[Throwable]
                if( errorStack !=null) {
                   error(s"HIVE ERROR :: ERROR STACK IS $errorStack :: ${errorStack.getLocalizedMessage()} ")
                   if(errorStack.getCause != null) 
                      error(s"HIVE ERROR ::   CAUSE IS ${errorStack.getCause} :: ${errorStack.getCause.getLocalizedMessage()} ")
                } else {
                  error("HIVE ERROR :: ErrorStack is not set ") 
                }
                
                val stackTraces = sessionState->("getStackTraces")
                if(stackTraces != null) {
                   stackTraces.asInstanceOf[java.util.Map[String, java.util.List[java.util.List[String]]]].foreach { case( stackName , stackTrace) => {
                     error( s"## Stack $stackName ")
                     stackTrace.foreach { ln => error(s"      ##${ln}")  }
                    }
                  }
                }
                val localErrs = sessionState->("getLocalMapRedErrors")
                if( localErrs != null) {
                	localErrs.asInstanceOf[java.util.HashMap[String,java.util.List[String]]].foreach { case(key,errLines)  => {
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
        } finally {
           driver.get.->("close")
           driver.get.->("destroy")
           driver.release
        }
    }
    
    
    /// XXX FIX ME
    /// output is not being returned 
    def readResults( response : CommandProcessorResponse, maxRows : Int ) = {
      if(response.getSchema != null) {
        response.getSchema.getFieldSchemas.foreach( field => {
            print(s"${field.getName}\t")
        })
      
      val tmpFile = sessionState.->("getTmpOutputFile").asInstanceOf[File]
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
    val mapRedStats = sessionState.->("getLastMapRedStatsList").asInstanceOf[java.util.ArrayList[MapRedStats] ]
    if( mapRedStats != null) {
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
