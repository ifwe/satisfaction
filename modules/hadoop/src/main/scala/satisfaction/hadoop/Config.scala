package satisfaction
package hadoop

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.conf.Configuration
import collection.JavaConversions._
import collection.JavaConverters._
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import java.io.ByteArrayInputStream
import java.io.FileNotFoundException
import java.io.File
import java.io.FileInputStream
import hdfs.Hdfs
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.commons.logging.LogFactory

/**
 *  Scala Object to handle initial configuration
 *   to be used
 */
object Config  extends Logging {
    def initHiveConf: HiveConf = {
        
        info( s" Java Version is ${System.getProperty("java.version")}" )
        info( s" Hadoop Major Version is ${ShimLoader.getMajorVersion()}" )
        val hc = new HiveConf(new Configuration(), this.getClass())
        
        val hadoopDir = hadoopConfDir 
        info(s"HADOOP Config Directory = $hadoopDir")
        if( hadoopDir != null && hadoopDir.exists  && hadoopDir.isDirectory) {
           HadoopResources.foreach( res => {
              val resFile = new File(hadoopDir.getPath + "/" + res)
              if(resFile.exists() ) {
                info(s" Adding resource ${resFile.getPath} ")
                hc.addResource( new FileInputStream(resFile),res)
              }
           } )
        } else {
          warn(" Invalid Hadoop Config directory")
        }
        
        val hiveDir = hiveConfDir
        info(s"Hive Config Directory = $hiveDir")
        if( hiveDir != null && hiveDir.exists  && hiveDir.isDirectory) {
           HiveResources.foreach( res => {
              val resFile = new File(hiveDir.getPath + "/" + res)
              if(resFile.exists() ) {
                info(s" Adding resource ${resFile.getPath} ")
                hc.addResource( new FileInputStream(resFile),res)
              }
           } )
        } else {
          warn(" Invalid Hive Config directory")
        } 

       val nameService = hc.get("dfs.nameservices")
       if(nameService != null) {
         hc.set("fs.defaultFS", s"hdfs://$nameService")
       }
       
       //// Override Retries    
       hc.setVar(HiveConf.ConfVars.HMSHANDLERATTEMPTS, "10")
       hc.setVar(HiveConf.ConfVars.HMSHANDLERINTERVAL, "3000")
       hc.setVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, "5")
       hc.setVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, "6")
       
       if(hc.getVar(HiveConf.ConfVars.PREEXECHOOKS).equals("org.apache.hadoop.hive.ql.hooks.ATSHook") ) {
          log.warn(" Overriding bogus Ambari Timeline Server ATSHook class")
          hc.setVar(HiveConf.ConfVars.PREEXECHOOKS, "")
          hc.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "")
          hc.setVar(HiveConf.ConfVars.ONFAILUREHOOKS, "")
       }
       
       /// AY YAH !!
       /// For guava dependencies, force the version that we package with the track
       /**
       hc.set("mapreduce.task.classpath.user.precedence","true");
       hc.set("mapreduce.job.user.classpath.first", "true");
       hc.set("mapreduce.user.classpath.first", "true");
       
       hc.setBoolean( MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true ) 
       * 
       */
       
       /*** 
        *  Set log4j libraries to info for some classes 
        * 
        * 
        */
       
       
       

       hc
    }
    
    
    /**
     * The location of local Hadoop config files
     */
    def hadoopConfDir : File = {
       if(sys.env.contains("HADOOP_CONF_DIR") ) {
          new File( sys.env("HADOOP_CONF_DIR"))   
       } else {
          if(sys.env.contains("HADOOP_HOME")) {
              new File( sys.env("HADOOP_HOME") + "/etc/hadoop")   
          } else {
             new File( "/usr/lib/hadoop/etc/hadoop" )
          }
       }
    }

    def hiveConfDir : File = {
       if(sys.env.contains("HIVE_CONF_DIR") ) {
          new File( sys.env("HIVE_CONF_DIR"))   
       } else {
          if(sys.env.contains("HIVE_HOME")) {
              new File( sys.env("HIVE_HOME") + "/conf")   
          } else {
            new File( "/usr/lib/hive/conf" )
          }
       }
    }
    
    /// Define all the specific resource paths to check
     val HadoopResources = Set( "core-site.xml",
    		  				"hdfs-site.xml",
    		  				"yarn-site.xml",
    		  				"mapred-site.xml")

    val HiveResources = Set( "hive-site.xml")

    val config = initHiveConf
    
    def apply( track : Track ) : HiveConf = {
      val thisConf = new HiveConf( config)
      
      HadoopResources.foreach { res => {
         try {
            thisConf.addResource( new ByteArrayInputStream( track.getResource( res).getBytes() ), res) 
         } catch {
           case unexpected : Throwable => 
             println(s"Couldn't find resource $res")
         }
        }
      }
      HiveResources.foreach { res => {
         try {
            thisConf.addResource( new ByteArrayInputStream( track.getResource( res).getBytes() ), res) 
         } catch {
           case notFound : FileNotFoundException => 
             println(s"Couldn't find  resource $res ")
         }
        }
      }
      track.hdfs = Hdfs.fromConfig(thisConf)
      
      ///// Set Hive AUX Jars Path
      println(" Current AuxJars is " +config.getAuxJars())
      val currentAux : Array[String] = if( config.getAuxJars != null) { 
           config.getAuxJars.split(",")}
        else {
          Array()
      }

      //// Add all jars which are in the Track's lib directory
      val newJars = track.hdfs.listFiles( track.libPath ) map ( _.path.toUri )
      //// Add the resources as well, so they get put to distributed Cache
      val newResources = track.hdfs.listFiles( track.resourcePath ) map ( _.path.toUri )

      val newAuxJars = if( track.trackProperties.getOrElse(Variable("satisfaction.override.hive.aux.jars.path"),"true").toBoolean )  {
        (newJars ++ newResources).mkString(",") } else {
        (newJars ++ newResources ++ currentAux).mkString(",") }
      info(s" Seting AuxJars Path to $newAuxJars ")
      thisConf.setAuxJars(newAuxJars)
      //// Need to set this as well ???
      thisConf.set("hive.aux.jars.path", newAuxJars)
      
      //// set the user if there 
      //// XXX Does this work ???
      if( track.trackProperties.contains("satisfaction.track.user.name") ) {
         thisConf.set("mapreduce.job.user.name", track.trackProperties.getProperty("satisfaction.track.user.name"))
      }
      
      //// Just go ahead and override the Hadoop Configuration ...
      track.trackProperties.assignments.foreach(  varass => {
         thisConf.set( varass.variable.name, varass.value.toString) 
      })
      //// Need to set this as well ???
      thisConf.set("hive.aux.jars.path", newAuxJars)
      
      thisConf
    }
    
    /**
     *  Provide an implicit conversion from Hadoop Configuration
     *    to our Witness class, to avoid dependencies on Hadoop library
     */
    implicit def Configuration2Witness( hadoopConf : Configuration ) : Witness = {
       new Witness(hadoopConf.iterator.map( entry => { 
             VariableAssignment[String]( Variable( entry.getKey ), entry.getValue )
       } ).toSet )
    }
    
    implicit def Witness2Configuration( witness : Witness ) : Configuration = {
      /// XXX TODO
       null 
    }

}