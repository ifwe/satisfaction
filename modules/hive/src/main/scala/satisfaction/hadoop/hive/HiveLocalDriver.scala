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
import _root_.org.apache.hadoop.hive.ql.exec.Utilities
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
import _root_.org.apache.hadoop.hive.ql.io.IOContext
import _root_.org.apache.hadoop.util.ReflectionUtils
import java.lang.reflect.Field
import _root_.org.apache.hadoop.io.WritableComparator
import _root_.org.apache.thrift.meta_data.FieldMetaData
import _root_.org.apache.hive.com.esotericsoftware.reflectasm.AccessClassLoader
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider
import org.apache.hadoop.filecache.DistributedCache


/**
 *  Executes Jobs locally, going directly through 
 *    the internal 'SessionState' interface
 */

class HiveLocalDriver( val hiveConf : HiveConf = new HiveConf( Config.config, classOf[HiveDriver] ) )
      ///extends satisfaction.hadoop.hive.HiveDriver with MetricsProducing with Progressable with Logging {
      extends satisfaction.hadoop.hive.HiveDriver with MetricsProducing  with Logging {
  
  
     val driver = new Releaseable[Wrapper]( factory = {
        val cl = this.getClass.getClassLoader() 
        try {
          val dr = Wrapper.withConstructor( "org.apache.hadoop.hive.ql.Driver", cl, Array[Class[_]]( classOf[HiveConf] ) , Array(  hiveConf ) )
          dr.->("init")
           dr
        }  catch {
          case unexpected : Throwable => {
            error(s"Unexpected error while creating HiveDriver ${unexpected.getMessage()} ", unexpected )
            throw unexpected
          } 
         } 
    } , finalizerFunc = { wrapper : Wrapper => {
      try {
         info(s" Calling close on our ApacheDriver Wrapper $wrapper ")
         wrapper.->("close")
      } catch {
         case unexpected : Throwable => {
          error(" Unexpected error trying to close Apache HiveDriver " , unexpected)
         } 
      }
    } } )
    
    override def close() = {
       info("HiveLocalDriver calling close")
       driver.release
       info(" Any Day Now, Any Way Now, I Shall Be Released !!!")
      
       /// Release the Metastore
       try {
         info(" Closing Current Hive, and closing the session state")
         Hive.closeCurrent()
         if(_sessionState != null) {
            sessionState.->("close")
            sessionState.execStatic("detachSession") 
         }
      
            ///ReflectionUtils.clearCache();
         val clearCacheMeth = classOf[ReflectionUtils].getDeclaredMethod("clearCache")
         clearCacheMeth.setAccessible(true)
         clearCacheMeth.invoke( null)
           
         IOContext.clear();
         
         val thisLoader = this.getClass.getClassLoader
         val outerMeth = thisLoader.getClass().getDeclaredMethod("getOuterLoader")
         outerMeth.setAccessible(true)
         val outerLoader = outerMeth.invoke( thisLoader)
         info(s" ThisLoader = $thisLoader OuterLoader = $outerLoader ")
         
         ///// WritableComparaters
         val comparatorsField : Field = classOf[WritableComparator].getDeclaredField("comparators")
         comparatorsField.setAccessible(true)
         
         val comparators : java.util.Map[Class[_],WritableComparator] = comparatorsField.get(null).asInstanceOf[java.util.Map[Class[_],WritableComparator]]

         ////info(s" WritableComparators are $comparators :: ${comparators.size} values ; This ClassLoader = ${this.getClass.getClassLoader} ")
         comparators.foreach({ case(klass : Class[_],comp : WritableComparator) => {
             info(s" Class is ${klass.getName} Class Loader = ${klass.getClassLoader}" )
             if( klass.getClassLoader() == thisLoader
                 || klass.getClassLoader() == outerLoader) {
                ///info(s" Removing class ${klass.getName} ")      
                comparators.remove(klass)
             }
         } })
         
         //// org.apache.thrift.meta_data.FieldMetaData
         val structMapField : Field = classOf[FieldMetaData].getDeclaredField("structMap")
         structMapField.setAccessible(true)
         
         //// Avoid ConcurrentModificationException
         val structMap : java.util.Map[Class[_],_] = structMapField.get(null).asInstanceOf[java.util.Map[Class[_],_]]
         ////info(s" thrift FieldMetaData structmap is  $structMap :: ${structMap.size} values ; This ClassLoader = ${this.getClass.getClassLoader} ")
         structMap.filter({ case(klass : Class[_], other) => {
             ///info(s" StructMap Class is ${klass.getName} Class Loader = ${klass.getClassLoader}" )
             klass.getClassLoader() == thisLoader  || klass.getClassLoader() == outerLoader
         } }).foreach( { case(klass : Class[_], other) => {
            ///info(s" Removing class ${klass.getName} ")      
            structMap.remove(klass)
         } })
        
         /// Can we get access ???
         val shutdownHookClass = Class.forName("java.lang.ApplicationShutdownHooks")
         val hooksField = shutdownHookClass.getDeclaredField("hooks")
         hooksField.setAccessible(true)
         val hooks : java.util.Map[Thread,Thread] = hooksField.get(null).asInstanceOf[java.util.Map[Thread,Thread]]
         
         if(hooks != null) {
           hooks.filter( { case( t1 : Thread, t2 : Thread) => {
              val contextLoader = t1.getContextClassLoader()
              info(s" Thread ContextLoader = $contextLoader ; ThisLoader = ${this.getClass.getClassLoader} ")
            (contextLoader == thisLoader 
             || contextLoader == outerLoader 
             || t1.getClass().getClassLoader() == thisLoader 
             || t1.getClass().getClassLoader() == outerLoader 
             || t2.getClass().getClassLoader() == thisLoader 
             || t2.getClass().getClassLoader() == outerLoader )
            } }).foreach( { case( t1: Thread, t2: Thread ) => {
              ///info(s" Removing Thread ${t1.getName} Shutdown Hook with context Loader")
              hooks.remove( t1)
            } })
         }
         
         
         ///// ReflectAsm AccessClassLoader
         val accessClassLoaderClass = Class.forName("org.apache.hive.com.esotericsoftware.reflectasm.AccessClassLoader")
         val accessClassLoadersField = accessClassLoaderClass.getDeclaredField("accessClassLoaders")
         accessClassLoadersField.setAccessible(true)
         
         val accessClassLoaders : java.util.List[ClassLoader] = accessClassLoadersField.get(null).asInstanceOf[java.util.List[ClassLoader]]
         accessClassLoaders.filter( { accessCl => {
             ///info(s"  AccessClassLoader = ${accessCl} Parent = ${accessCl.getParent()} ")
             accessCl.getParent() == thisLoader || accessCl.getParent() == outerLoader
         }}).foreach( { accessCl => {
             ///info(s" Removing AccessClassLaoder $accessCl ")
             accessClassLoaders.remove(accessCl)
         }})
         
         //// ObjectInspectorFactor
         val objInspectorFactoryClass = Class.forName("org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory")
         val cachedInspectorField = objInspectorFactoryClass.getDeclaredField("cachedUnionStructObjectInspector")
         cachedInspectorField.setAccessible(true)
         
         val cachedInspectors : java.util.Map[java.util.List[_],_] = cachedInspectorField.get(null).asInstanceOf[java.util.Map[java.util.List[_],_]]
         cachedInspectors.filter( { case(klist,v) => {
            ////info(s" ObjecInspectorFactory KeyList = $klist Value= $v")
            val keyHasLoader = klist.filter( { k =>{ 
              k.getClass().getClassLoader() == thisLoader || k.getClass().getClassLoader() == outerLoader 
             }}).size > 0
            ( keyHasLoader
              || v.getClass().getClassLoader() == thisLoader 
              || v.getClass().getClassLoader() == outerLoader )
         }}).foreach( { case(k,v) => {
             ////info(s" Removing ObjectInspector $k ") 
             cachedInspectors.remove( k)
         }})

         ///// Use 
         ////org.apache.hadoop.hdfs.PeerCache.getInstance(0,0).close()
        
       } catch {
         case unexpected : Throwable => {
          error(" Unexpected error trying to close Hive and SessionState " , unexpected)
         } 
       }
      

      try {

        Utilities.runtimeSerializationKryo.remove();

        val cloneQueryPlanKryoField = classOf[Utilities].getDeclaredField("cloningQueryPlanKryo")
        cloneQueryPlanKryoField.setAccessible(true)
      
        val staticThreadLocal= cloneQueryPlanKryoField.get( null).asInstanceOf[ThreadLocal[_]] /// Remove reference to Kryo from the thread Local
        staticThreadLocal.remove
      } catch {
        case unexpected : Throwable => {
          error(" Unexpected error trying to clean threadLocal cloningQueryPlanKryo " , unexpected)
        } 
      }


      try {
        val thisClassLoader = this.getClass().getClassLoader
        thisClassLoader match {
          case closable : java.io.Closeable => {
             info(s" HiveLocalDriver :: Closing Closable ClassLoader $thisClassLoader $closable ")      
             closable.close()
          }
          case _ => {
             info(" HiveLocalDriver :: Our classloader was not closable") 
          }
        }
      } catch {
        case unexpected : Throwable => {
          error(" Unexpected error trying to close Isolated ClassLoader " , unexpected)
        } 
      }
    }
    
    
    override def setProperty( prop : String , propValue : String) = {
      this.hiveConf.set( prop, propValue)
    }
    
    /**
     *  Implement the Hive 'add file <resource>' command
     */
    def addResource( resourceType : String, resource : String ) : Boolean = {
      val hiveConfVar = {
        resourceType.trim.toLowerCase match {
          case "file" =>  HiveConf.ConfVars.HIVEADDEDFILES 
          case "files" =>  HiveConf.ConfVars.HIVEADDEDFILES 
          case "jar" =>  HiveConf.ConfVars.HIVEADDEDJARS
          case "jars" =>  HiveConf.ConfVars.HIVEADDEDJARS
          case "archive" =>  HiveConf.ConfVars.HIVEADDEDARCHIVES
          case "archives" =>  HiveConf.ConfVars.HIVEADDEDARCHIVES
          case _ => {
            error(s"Unknown resource type $resourceType ")
            return false
          }
        }
      }
      
      val hdfsURI = """^hdfs://(.+)""".r
      val fileURI = """"^file://(.+)""".r
      val absoluteFile = "^/(.*)".r
      val filename : String = {
        resource match {
          case hdfsURI(uri) => {
             resource
          }    
          case fileURI(f) => {
             resource
          }    
          case absoluteFile(f) => {
            s"file://$resource" 
          }
          case _ => {
            s"file://${System.getProperty("user.dir")}/$resource" 
          }
        }
      }
      
      info(s" Adding resource $filename  TO $hiveConfVar ")
      val checkPath = hiveConf.getVar( hiveConfVar )
      if( checkPath == null || checkPath.trim.length == 0) {
        hiveConf.setVar( hiveConfVar, filename)
      } else {
        hiveConf.setVar( hiveConfVar, s"${checkPath},${filename}")
      }
      
      true 
    }
    /**
    public CommandProcessorResponse run(String command) {
    SessionState ss = SessionState.get();
    command = new VariableSubstitution().substitute(ss.getConf(),command);
    String[] tokens = command.split("\\s+");
    SessionState.ResourceType t;
    if (tokens.length < 2
        || (t = SessionState.find_resource_type(tokens[0])) == null) {
      console.printError("Usage: add ["
          + StringUtils.join(SessionState.ResourceType.values(), "|")
          + "] <value> [<value>]*");
      return new CommandProcessorResponse(1);
    }
    for (int i = 1; i < tokens.length; i++) {
      String resourceFile = ss.add_resource(t, tokens[i]);
      if(resourceFile == null){
        String errMsg = tokens[i]+" does not exist.";
        return new CommandProcessorResponse(1,errMsg,null);
      }
    }
    * 
    */
    
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
    lazy val sessionState: Wrapper  = {
       info(s" Starting SessionState !!!")
       ///SessionState.start( new HiveConf(hiveConf))
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
       close
    }
    
    override def executeQuery(query: String): Boolean = {
        try {

            //// Hack to support Hive 'ADD FILE ' command
            ////  not quite fully compliant, but will move to 
            ////  new HiveServer next release
            if( query.toLowerCase().trim().startsWith("add")) {
               val cmdArr = query.split(" ") 
               if(cmdArr.length != 3) {
                  error(" Add File command takes resource type and file name ") 
                  return false
               }
               return addResource(cmdArr(1), cmdArr(2) )
            }
            val response : CommandProcessorResponse = HiveLocalDriver.retry (5) {
                sessionState.execStatic("setCurrentSessionState", sessionState.wrapped)

                val resp = driver.get.->("run",query).asInstanceOf[CommandProcessorResponse]
                resp
            }

            info(s"Response Code ${response.getResponseCode} :: SQLState ${response.getSQLState} ")
            if (response.getResponseCode() != 0) {
                error(s"HIVE_DRIVER Driver Has error Message ${driver->("getErrorMsg")}")
                error(s"Error while processing statement: ${response.getErrorMessage()} ${response.getSQLState()} ${response.getResponseCode()}" );
                
                val driverClass = driver.get.wrapped.getClass
                
                /// XXX Use wrapper class ...
                val errorMember =  driverClass.getDeclaredFields.filter( _.getName().endsWith("Error"))(0)
               
                errorMember.setAccessible(true)
                
                val errorStack : Throwable = errorMember.get( driver.get.wrapped).asInstanceOf[Throwable]
                if( errorStack !=null) {
                   error(s"HIVE ERROR :: ERROR STACK IS $errorStack :: ${errorStack.getLocalizedMessage()} ", errorStack)
                   if(errorStack.getCause != null) 
                      error(s"HIVE ERROR ::   CAUSE IS ${errorStack.getCause} :: ${errorStack.getCause.getLocalizedMessage()} ",errorStack.getCause)
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
           driver.get.->("close")
           driver.get.->("destroy")
           driver.release
                return false
            case unexpected : Throwable => 
                error(s"Dammit !!! Unexpected SQLException ${unexpected.getLocalizedMessage} ", unexpected)
           driver.get.->("close")
           driver.get.->("destroy")
           driver.release
                throw unexpected
        } finally {
          info(" What happens if we don't close the driver each time ??? ")
           ///driver.get.->("close")
           ///driver.get.->("destroy")
           ///driver.release
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
    ///val mapRedStats = sessionState.->("getLastMapRedStatsList").asInstanceOf[java.util.ArrayList[MapRedStats] ]
     info(" Not updating JobMetrics for now ")
    /***
    val mapRedStats = sessionState.getLastMapRedStatsList()
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
      * 
    }
      */
     
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
        case clusterException if clusterException.getMessage() != null
        		&& clusterException.getMessage().contains("Cannot initialize Cluster. Please check your configuration") => {
           checkCluster() 
           println(s" Number of retries = $cnt")
           cnt += 1
           if( cnt == numRetries) {
              throw clusterException
           }
        }
        case unexpected : Throwable => {
          throw unexpected
        }
      }
    }
    null.asInstanceOf[T]
  }
  
  
  def checkCluster() = {
     println(" Checking Cluster ClientProtocol Providers ")
     val clusterClass = Class.forName( "org.apache.hadoop.mapreduce.Cluster")
     val frameworkLoaderField = clusterClass.getDeclaredField("frameworkLoader")
     frameworkLoaderField.setAccessible(true)
     val frameworkLoader = frameworkLoaderField.get(null)
     if( frameworkLoader == null) {
        println(" FrameworkLoader is null for some reason ... try to set it   ")
        val clientProvider = java.util.ServiceLoader.load(classOf[ClientProtocolProvider])
        frameworkLoaderField.set( null, clientProvider)
     } else {
       println( s" FrameworkLooader is $frameworkLoader ; Now What do we do ????")
     }
  }

}