package satisfaction
package hadoop.hive 

import java.net.URL
import scala.collection.JavaConversions.seqAsJavaList
import _root_.org.apache.commons.logging.Log
import _root_.org.apache.hadoop.hive.conf.HiveConf
import satisfaction.Logging
import satisfaction.Track
import satisfaction.Witness.Witness2Properties
import satisfaction.hadoop.CachingTrackLoader
import _root_.org.apache.hadoop.hive.ql.metadata.Hive
import satisfaction.util.classloader.IsolatedClassLoader
import satisfaction.fs.LocalFileSystem
import satisfaction.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.conf.Configuration

/**
 *  Trait for class which can executes
 *    Hive Queries,
 *     and interact with Hive ( i.e parse syntax, plan queries,
 *         define dependencies )
 *
 *   Can be both local, using Hive Driver implementation,
 *     or remote, using HiveServer2 JDBC client
 */
trait HiveDriver extends java.io.Closeable {

    def useDatabase(dbName: String) : Boolean 

    def executeQuery(query: String): Boolean
    
    def setProperty( prop : String, propValue : String )
    
    def abort() 
    
    def close() 

}




object HiveDriver extends Logging {

  def apply(hiveConfRef: HiveConf)(implicit track : Track): HiveDriver = {
    try {
      /**
      val parentLoader = if (Thread.currentThread.getContextClassLoader != null) {
        Thread.currentThread.getContextClassLoader
      } else {
        hiveConf.getClassLoader
      }
      * 
      */
      /// Create a new HiveConf, so that we don't have reference to global objects,
      //// and we can get garbage collected
      val hiveConf = new HiveConf(hiveConfRef)
      info( s" Current Thread = ${Thread.currentThread.getName} ThreadLoader = ${Thread.currentThread.getContextClassLoader}  HiveConfLoader = ${hiveConf.getClassLoader} This loader = ${this.getClass.getClassLoader} ")
      ////  XXX What should the parent loader be 
      ///val parentLoader = classOf[HiveDriver].getClassLoader()
      val parentLoader = hiveConf.getClassLoader
      
      //// XXX Centralize somewhere
       hiveConf.setVar(HiveConf.ConfVars.HMSHANDLERATTEMPTS, "10")
       hiveConf.setVar(HiveConf.ConfVars.HMSHANDLERINTERVAL, "3000")
       hiveConf.setVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, "5")
       hiveConf.setVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, "6")
       
       if(hiveConf.getVar(HiveConf.ConfVars.PREEXECHOOKS).equals("org.apache.hadoop.hive.ql.hooks.ATSHook") ) {
          log.warn(" Overriding bogus Ambari Timeline Server ATSHook class")
          hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "")
          hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "")
          hiveConf.setVar(HiveConf.ConfVars.ONFAILUREHOOKS, "")
       }
      
      
      info(s" ParentLoader = ${parentLoader} ")
      val auxJars = hiveConf.getAuxJars
     
      info( s" Track libPath is ${track.libPath}")
      info( s" Track resourcePath is ${track.resourcePath}")
      val urls =  track.listLibraries 
      val resources =  track.listResources
      val exportFiles = ( urls ++ resources)
      
      val isolateFlag = track.trackProperties.getProperty("satisfaction.classloader.isolate","true").toBoolean
      val urlClassLoader = if( isolateFlag) {
             
         val localPath = track.trackPath.toUri.getPath() /// remove hdfs:// scheme
         val cacheBase = track.trackProperties.getProperty("satisfaction.track.cache.path" , "/var/log/satisfaction-cache-root")

         val cachePath  = new Path(cacheBase) / localPath / "lib"
       
         val localFs = LocalFileSystem()
         if( !localFs.exists(cachePath) ) { localFs.mkdirs( cachePath) }
         info(s" Using IsolatedClassLoader with a cachePath of $cachePath")
         
         /// XXX Store as static resource
         val frontLoadClasses =  List("org.apache.hadoop.hive.ql.*", 
    		  "satisfaction.hadoop.hive.HiveLocalDriver", 
    		  "satisfaction.hadoop.hive.HiveLocalDriver.*", 
    		  "satisfaction.hadoop.hive.*", 
    		  "satisfaction.hadoop.hdfs.*",
    		  "org.apache.hadoop.hive.ql.Driver",
    		  "org.apache.hadoop.hive.ql.Driver.*",
    		  "org.apache.hadoop.hive.ql.exec.*",
    		  "org.apache.hadoop.hive.ql.exec.Task.*",
    		  "org.apache.hadoop.hive.ql.exec.Utilities",
    		  "org.apache.hadoop.hive.ql.exec.Utilities.*",
    		  "org.apache.hadoop.hive.ql.exec.DDLTask.*",
    		  "org.apache.hadoop.hive.ql.exec.TaskRunner.*",
    		  "org.apache.hadoop.hive.ql.session.SessionState",
    		  "org.apache.hadoop.hive.ql.session.SessionState.*",
    		  "org.apache.op.hive.ql.session.SessionState.*",
    		  "brickhouse.*",
              "org.apache.hive.com.esotericsoftware.kryo.*",
    		  "org.apache.hadoop.util.ReflectionUtils",
    		  "org.apache.hadoop.util.ReflectionUtils.*",
    		  "org.apache.hadoop.io.WritableComparator",
    		  "org.apache.hadoop.io.WritableComparator.*",
    		  "org.apache.hadoop.io.compress.CompressionCodecFactory",
    		  "org.apache.hadoop.io.compress.CompressionCodecFactory.*",
    		  "org.apache.hadoop.yarn.*",
    		  "org.apache.hadoop.mapreduce.*",
    		  "satisfaction.Logging",
    		  "satisfaction.Logging.*",
    		  "com.tagged.udf.*",
    		  "com.tagged.hadoop.hive.*")
         val backLoadClasses = List(
                  "satisfaction.hadoop.hive.HiveSatisfier",
                  "org.apache.hadoop.hive.conf.*",
    		      "org.apache.hive.common.*",
    		      "org.apache.hadoop.hive.common.*",
                  "org.apache.commons.logging.*",
                  "org.apache.hadoop.hbase.*",
                  "org.apache.hadoop.mapreduce.Cluster",
                  "org.apache.hadoop.mapreduce.protocol.*",
                  "org.apache.hadoop.mapreduce.util.*",
                  "satisfaction.util.*"
                  ////"org.apache.hadoop.hive.ql.metadata.*",
    		      ///"org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper",
    		      ///"org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.*",
                  ///"org.apache.hadoop.hive.metastore.*",
                  ///"org.apache.hadoop.hive.ql.plan.api.*",
                  ///"org.apache.hadoop.hive.ql.metadata.*",
                  //"org.apache.*HiveMetaStoreClient.*",
                  ///"org.apache.*IMetaStoreClient.*",
                  ///"org.apache.hadoop.hive.metastore.*"
                  ///"org.apache.hadoop.hive.ql.lockmgr.*",
                  ////"org.apache.hadoop.hive.metastore.api.*",
                  ///"org.apache.*HiveMetaHookLoader.*")
                  )
         val isolatedClassLoader = new IsolatedClassLoader( exportFiles.map( _.toUri.toURL).toArray[URL], 
    		  	parentLoader,
    		  	frontLoadClasses,
    		  	backLoadClasses, 
    		  	hiveConf,
    		  	cachePath.pathString);
         isolatedClassLoader.registerClass(classOf[HiveDriver]);
         isolatedClassLoader.registerClass(classOf[HiveConf]);
         isolatedClassLoader.registerClass(classOf[HiveConf.ConfVars]);
         isolatedClassLoader.registerClass(classOf[org.apache.hadoop.mapred.JobConf]);
         isolatedClassLoader.registerClass(classOf[Configuration]);
         
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
          java.net.URLClassLoader.newInstance( exportFiles.map( _.toUri.toURL).toArray[URL] )
      }
      

      val auxJarPath = exportFiles.map( _.toUri.toString ).mkString(",")
      
      info(" Using AuxJarPath " + auxJarPath)
      hiveConf.setAuxJars( auxJarPath)
      hiveConf.set("hive.aux.jars.path", auxJarPath)
      hiveConf.setClassLoader(urlClassLoader)

      //// XXX Move to Scala reflection ...
      info( "Instantiating HiveLocalDriver")
      //// XXX Specify as track property ..
      val localDriverClass: Class[_] = urlClassLoader.loadClass("satisfaction.hadoop.hive.HiveLocalDriver")
      info( s" Local Driver Class is $localDriverClass ")
      val constructor = localDriverClass.getConstructor(hiveConf.getClass() )
      val satisfactionHiveConf = new SatisfactionHiveConf(hiveConf)
      satisfactionHiveConf.setClassLoader( urlClassLoader)
      
      val hiveLocalDriver = constructor.newInstance(satisfactionHiveConf )
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
      debug(s" Loading HiveConf class $className with ClassLoader ${getClassLoader}" ) 
      
      getClassLoader.loadClass(className)
  }
  
  
}

/**
class HiveDriverHook extends HiveDriverRunHook with Logging {
     /**
   * Invoked before Hive begins any processing of a command in the Driver,
   * notably before compilation and any customizable performance logging.
   */
   def preDriverRun(hookContext : HiveDriverRunHookContext)  = {
     
      info("HIVE_DRIVER :: PRE DRIVER RUN :: " + hookContext.getCommand())
      ////SessionState.getConsole.printInfo("HIVE_DRIVER :: PRE DRIVER RUN :: " + hookContext.getCommand())
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
* 
*/
  
