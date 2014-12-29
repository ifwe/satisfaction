package satisfaction.hadoop

import satisfaction.track.DefaultTrackLoader
import satisfaction.track.TrackFactory
import satisfaction.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import satisfaction.Witness
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import satisfaction.Logging
import satisfaction.hadoop.hdfs.CacheJarURLStreamHandlerFactory

class CachingTrackLoader(trackFactory : TrackFactory, trackProps : Map[String,String] )  extends DefaultTrackLoader(trackFactory,trackProps) with Logging {
  
  
    /**
      *  Create a CachingClassLoader, to reduce stress on the HDFS filesystem,
      *    and reduce spurious errors 
      */
     override def createClassLoader( trackPath : Path ) : ClassLoader = {
       val configMap = trackFactory.defaultConfig match {
         case Some(config) => (config.raw ++ trackProps)
         case None => (trackProps)
       }
       val hiveConf = new HiveConf()
       configMap.foreach( {case(k,v) => {
           hiveConf.set( k,v) 
       } })
       
       
       val cachePath  = CachingTrackLoader.getCachePath( trackPath)
       info(s" Using ${cachePath} instead of trackPath $trackPath ")
       
       val cachingStreamHandlerFactory = new CacheJarURLStreamHandlerFactory( hiveConf, cachePath.toString)
       
       /// For now, don't bother isolating at the track level
       new java.net.URLClassLoader(jarURLS(trackFactory.hdfs, trackPath  ),this.getClass.getClassLoader, cachingStreamHandlerFactory)

     }

}

object CachingTrackLoader  {
  
     def getCachePath( trackPath : Path) : Path = {
       //// Place cache in local directory , but append the track Path
       val localPath = trackPath.toUri.getPath() /// remove hdfs:// scheme
       

       val cachePath  = Path( System.getProperty("user.dir") ) / "cache" / localPath
       
       cachePath
     }
}