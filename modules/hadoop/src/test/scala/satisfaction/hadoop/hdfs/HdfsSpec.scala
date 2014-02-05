package com.klout
package satisfaction
package hadoop
package hdfs

import org.specs2.mutable._
import com.klout.satisfaction.Witness
import com.klout.satisfaction._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import io._
import satisfaction.fs.Path
import org.apache.hadoop.conf.Configuration

@RunWith(classOf[JUnitRunner])
class HdfsSpec extends Specification {
  
  
    
    "Hdfs" should {
        "create URLS starting with hdfs" in {
          //// XXX use MiniFS for unit testing ...
          ///val hdfsUrl = new java.net.URL("hdfs://jobs-dev-hnn/user/satisfaction/track/Sample/version_2.1/satisfaction.properties")
          /// Externalize configuration 
          val hdfsUrl = new java.net.URL("hdfs://nameservice1/user/satisfaction/track/Sample/version_2.1/satisfaction.properties")
         
          val stream = hdfsUrl.openStream
          val props  = Substituter.readProperties( stream)
          
          true
        }
        
        
        "List files" in {
          val hdfs = Hdfs.fromConfig(HdfsSpec.clientConfig)
          
          val path = new Path("hdfs://nameservice1/data/hive/maxwell/actor_action")
          
          
          hdfs.listFiles( path ).foreach( fs => {
            System.out.println(s" Path is ${fs.getPath} ${fs.getSize} ${fs.lastAccessed}  ")
          } )
          
          val pathToday =  path / "20140116"
          hdfs.listFilesRecursively( pathToday ).foreach( fs => {
            System.out.println(s" Path is ${fs.getPath} ${fs.getSize} ${fs.lastAccessed}  ")
          } )
          
          true
        }
        
        
        "access nameservice1" in {
          
          val testConf : Configuration = HdfsSpec.clientConfig
          testConf.writeXml(System.out)
          val haHdfs = Hdfs.fromConfig( testConf)
          
          val nsPath = new Path("hdfs://nameservice1/user/hive/warehouse/bi_maxwell.db")
          haHdfs.listFiles( nsPath ).foreach( fs => {
            System.out.println(s" Path is ${fs.getPath} ${fs.getSize} ${fs.lastAccessed}  ")
          } )
          
        }

    }

}

object HdfsSpec {
    
    def clientConfig: Configuration = {
      val conf = new Configuration
      val testPath = System.getProperty("user.dir") + "/modules/hadoop/src/test/resources/config/hdfs-site.xml"
      conf.addResource( new java.io.File(testPath).toURI().toURL())
      
      
       val nameService = conf.get("dfs.nameservices")
       if(nameService != null) {
         conf.set("fs.defaultFS", s"hdfs://$nameService")
       }
      conf
    }
}