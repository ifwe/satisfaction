package com.klout
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

/**
 *  Scala Object to handle initial configuration
 *   to be used
 */
object Config {
    def initHiveConf: HiveConf = {
        print(ShimLoader.getMajorVersion())
        val hc = new HiveConf(new Configuration(), this.getClass())
        
        val hadoopDir = hadoopConfDir 
        println(s"HADOOP Config Directory = $hadoopDir")
        if( hadoopDir != null && hadoopDir.exists  && hadoopDir.isDirectory) {
           HadoopResources.foreach( res => {
              val resFile = new File(hadoopDir.getPath + "/" + res)
              if(resFile.exists() ) {
                println(s" Adding resource ${resFile.getPath} ")
                hc.addResource( new FileInputStream(resFile))
              }
           } )
        } else {
          println(" Invalid Hadoop Config directory")
        }
        
        val hiveDir = hiveConfDir
        println(s"Hive Config Directory = $hiveDir")
        if( hiveDir != null && hiveDir.exists  && hiveDir.isDirectory) {
           HiveResources.foreach( res => {
              val resFile = new File(hiveDir.getPath + "/" + res)
              if(resFile.exists() ) {
                println(s" Adding resource ${resFile.getPath} ")
                hc.addResource( new FileInputStream(resFile))
              }
           } )
        } else {
          println(" Invalid Hive Config directory")
        } 

       val nameService = hc.get("dfs.nameservices")
       if(nameService != null) {
         hc.set("fs.defaultFS", s"hdfs://$nameService")
       }
        
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
            null
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
            null
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
      println( s" Hadoop HADOOP_HOME= ${sys.env("HADOOP_HOME")}")
      val thisConf = new HiveConf( config)
      
      HadoopResources.foreach { res => {
         try {
            thisConf.addResource( new ByteArrayInputStream( track.getResource( res).getBytes() ), res) 
         } catch {
           case unexpected : Throwable => 
             println("Trouble finding resource ")
             unexpected.printStackTrace(System.out)
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