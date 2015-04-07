package satisfaction
package hadoop
package hdfs

import org.specs2.mutable._
import satisfaction.Witness
import satisfaction._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import io._
import satisfaction.fs.Path
import org.apache.hadoop.conf.Configuration
import satisfaction.fs.LocalFileSystem

@RunWith(classOf[JUnitRunner])
class HdfsSpec extends Specification {
  
  
    
    "Hdfs" should {
        "create URLS starting with hdfs" in {
          //// XXX use MiniFS for unit testing ...
          /// Externalize configuration 
          val hdfsUrl = new java.net.URL("hdfs://dhdp2/user/satisfaction/track/Sample/version_2.1/satisfaction.properties")
         
          val stream = hdfsUrl.openStream
          val props  = Substituter.readProperties( stream)
          
          true
        }
        
        
        "List files" in {
          val hdfs = Hdfs.fromConfig(HdfsSpec.clientConfig)
          
          val path = new Path("hdfs:///data/ramblas/event_log")
          
          
          hdfs.listFiles( path ).foreach( fs => {
            System.out.println(s" Path is ${fs.path} ${fs.size} ${fs.lastAccessed}  ")
          } )
          
          val pathToday =  path / "20140429"
          hdfs.listFilesRecursively( pathToday ).foreach( fs => {
            System.out.println(s" Recursive Path is ${fs.path} ${fs.size} ${fs.lastAccessed}  ")
          } )

          hdfs.listFilesRecursively( path ).foreach( fs => {
            System.out.println(s" Path is ${fs.path} ${fs.size} ${fs.lastAccessed}  ")
          } )
          
          true
        }
        
        
        "access nameservice1" in {
          
          val testConf : Configuration = HdfsSpec.clientConfig
          testConf.writeXml(System.out)
          val haHdfs = Hdfs.fromConfig( testConf)
          
          val nsPath = new Path("hdfs://dhdp2/user/ramblas/lib")
          haHdfs.listFiles( nsPath ).foreach( fs => {
            System.out.println(s" Path is ${fs.path} ${fs.size} ${fs.lastAccessed}  ")
          } )
          
        }
        
        
        "read and write file" in {
           val hdfs = Hdfs.fromConfig( HdfsSpec.clientConfig)
           
           val brPath = Path("hdfs://dhdp2/user/satisfaction/track/DauBackfill/version_0.2/auxJar/brickhouse-0.7.0-jdb-SNAPSHOT.jar")

             val readFile = hdfs.readFile( brPath)
          
        }
        
        "read and write text file" in {
           val hdfs = Hdfs.fromConfig( HdfsSpec.clientConfig)
           
           val brPath = Path("hdfs://dhdp2/user/satisfaction/track/DauBackfill/version_0.2/satisfaction.properties")

             val readFile = hdfs.readFile( brPath)
             
             println(" Text file is  " + readFile)
          
        }
        
        
        "copy To Local" in {
          val brPath = Path("hdfs://dhdp2/user/satisfaction/track/DauBackfill/version_0.2/auxJar/brickhouse-0.7.0-jdb-SNAPSHOT.jar")
            
          val localPath =  Path("/tmp/hdfsTest" + System.currentTimeMillis()) / "brickhouse.jar"
          val localFS = LocalFileSystem
          
          val hdfs = Hdfs.fromConfig( HdfsSpec.clientConfig)
           
          hdfs.copyToFileSystem( localFS, brPath, localPath)
          
          val checkFile = new java.io.File( localPath.parent.toString )
          checkFile.exists must_== true
          checkFile.isDirectory must_== true

          val checkJar = new java.io.File( localPath.toString )
          checkJar.exists must_== true
          checkJar.isFile must_== true

          val lstat = localFS.getStatus(localPath)
          println( " JAr Size is " + lstat.size)
          lstat.size must_!= 0
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