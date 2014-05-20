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

/**
 *  Scala Object to handle initial configuration
 *   to be used
 */
object Config {
    def initHiveConf: HiveConf = {
        print(ShimLoader.getMajorVersion())
        val hc = new HiveConf(new Configuration(), this.getClass())
        
        return hc
    }

    val config = initHiveConf
    
    def apply( track : Track ) : HiveConf = {
      println( s" Hadoop HADOOP_HOME= ${sys.env("HADOOP_HOME")}")
      val thisConf = new HiveConf( config)
      
      /// Define all the specific resource paths to check
      val hadoopResources = Set( "core-site.xml",
    		  				"hdfs-site.xml",
    		  				"yarn-site.xml",
    		  				"mapred-site.xml",
    		  				"hive-site.xml"
    		  		)
    		  		
      hadoopResources.foreach { res => {
         try {
            thisConf.addResource( new ByteArrayInputStream( track.getResource( res).getBytes() ), res) 
         } catch {
           case unexpected : Throwable => 
             println("Trouble finding resource ")
             unexpected.printStackTrace(System.out)
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