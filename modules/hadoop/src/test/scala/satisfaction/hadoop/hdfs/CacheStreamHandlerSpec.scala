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
import java.net.URLClassLoader
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.hdfs.web.URLConnectionFactory
import java.io.PrintWriter

@RunWith(classOf[JUnitRunner])
class CacheStreamHandlerSpec extends Specification {
  
  
    
    "CacheStreamHandler" should {
      /**
        "load hbase-default.xml" in {
          
          implicit val hiveConf = new HiveConf(Config.config)
         
          val cacheStreamHandler = new CacheJarURLStreamHandlerFactory( hiveConf, System.getProperty("user.dir") + "/cache")
          
          val hdfs = new Hdfs("hdfs://dahdp2nn01")
          val libFiles = hdfs.listFiles( new Path("/user/satisfaction/track/Pinkman/version_0.5.5/lib"))
          libFiles.foreach( fs => {
              println(s"  LIB PATH  ${fs.path} ") 
          } )
          
          val classLoaderURLS = libFiles.map( fs => { new java.net.URL( fs.path.toString ) }).toArray
          
          val urlClassLoader = new URLClassLoader( classLoaderURLS, this.getClass.getClassLoader, cacheStreamHandler )
          ////val urlClassLoader = new URLClassLoader( classLoaderURLS, this.getClass.getClassLoader )
          
          val hbaseXmlStream= urlClassLoader.getResourceAsStream("hbase-default.xml")
          
          val reader= new BufferedReader( new InputStreamReader(hbaseXmlStream))
          var l : String  = reader.readLine
          while(  l != null ) {
              println(l)  
              l = reader.readLine
          }
          
        }
        
        "load hbase-default.xml from URL" in {
          
          
          System.setProperty("sun.misc.URLClassPath.debug", "true")
          
          implicit val hiveConf = new HiveConf(Config.config)
         
          val cacheStreamHandler = new CacheJarURLStreamHandlerFactory( hiveConf, System.getProperty("user.dir") + "/cache" )
          ////java.net.URL.setURLStreamHandlerFactory(cacheStreamHandler);
          
          val hdfs = new Hdfs("hdfs://dahdp2nn01")
          val libFiles = hdfs.listFiles( new Path("/user/satisfaction/track/Pinkman/version_0.5.5/lib"))
          libFiles.foreach( fs => {
              println(s"  LIB PATH  ${fs.path} ") 
          } )
          
          val classLoaderURLS = libFiles.map( fs => { new java.net.URL( fs.path.toString ) }).toArray
          
          val urlClassLoader = new URLClassLoader( classLoaderURLS, this.getClass.getClassLoader, cacheStreamHandler )
          ////val urlClassLoader = new URLClassLoader( classLoaderURLS, this.getClass.getClassLoader )
          
          
         
          val xmlURL = urlClassLoader.findResource( "hbase-default.xml")
          ///val xmlURL = new java.net.URL("jar:hdfs://dahdp2nn01/user/satisfaction/track/Pinkman/version_0.5.5/lib/hbase-common-0.98.0.2.1.2.0-402-hadoop2.jar!/hbase-default.xml")
          println(s" HBaseDefault URL = $xmlURL ")
          
          

          val hbaseXmlURLConn = xmlURL.openConnection()
          hbaseXmlURLConn.connect
          
          println(s" HBaseDefault commection = $hbaseXmlURLConn ")

          val hbaseXmlStream = hbaseXmlURLConn.getInputStream
          
          val reader= new BufferedReader( new InputStreamReader(hbaseXmlStream))
          var l : String  = reader.readLine
          while(  l != null ) {
              println(l)  
              l = reader.readLine
          }
          
        }
          
          **/
        "Instantiate HBaseConfiguration" in {
          
          implicit val hiveConf = new Configuration(Config.config)
         
          val cacheStreamHandler = new CacheJarURLStreamHandlerFactory( hiveConf, System.getProperty("user.dir") + "/cache")
          
          val hdfs = new Hdfs("hdfs://dahdp2nn01")
          val libFiles = hdfs.listFiles( new Path("/user/satisfaction/track/Pinkman/version_0.5.5/lib"))
          libFiles.foreach( fs => {
              println(s"  LIB PATH  ${fs.path} ") 
          } )
          
          val classLoaderURLS = libFiles.map( fs => { new java.net.URL( fs.path.toString ) }).toArray
           
          
        val urlClassLoader = new URLClassLoader( classLoaderURLS, this.getClass.getClassLoader, cacheStreamHandler )
         ////val urlClassLoader = new URLClassLoader( classLoaderURLS, this.getClass.getClassLoader )
          
          
          val hbaseConfigClass = urlClassLoader.loadClass( "org.apache.hadoop.hbase.HBaseConfiguration")
         
          
          println(s" HBase class is $hbaseConfigClass ")
          
          val hbaseVersionInfoClass = urlClassLoader.loadClass("org.apache.hadoop.hbase.util.VersionInfo")
          println(s" HBase Version info class is $hbaseVersionInfoClass ")
          
          val printMethod = hbaseVersionInfoClass.getDeclaredMethod("writeTo", classOf[java.io.PrintWriter] )
          val versionMethod = hbaseVersionInfoClass.getDeclaredMethod("getVersion" )
          
          val pw : PrintWriter = new PrintWriter(System.out)
          
          printMethod.invoke( null, pw )
          
          
          val hbVer = versionMethod.invoke( null ).toString
          println(s" HBase version is $hbVer")
          
          val hbaseVersionInfo = hbaseVersionInfoClass.newInstance
          
          
          println(s" HBaseVersion is ${hbaseVersionInfo} ")
          
          
          val createMeth = hbaseConfigClass.getDeclaredMethod("create",  classOf[Configuration] )
          
          hiveConf.set("hbase.defaults.for.version", "0.98.0.2.1.2.0-402-hadoop2")
          
          val createdConfig = createMeth.invoke(null, hiveConf)
          
          true
        }
        

    }

}
