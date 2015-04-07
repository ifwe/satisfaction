package satisfaction.track

import satisfaction.fs.Path
import satisfaction.Track
import satisfaction.Logging
import satisfaction.fs.FileSystem

/**
 *  Make TrackLoader a Trait, so that we can use custom classloaders 
 *    and creation methods later 
 */
trait TrackLoader {

     def createClassLoader( trackPath : Path ) : ClassLoader
     
     def loadTrackClass( trackClassName : String, classLoader : ClassLoader )  : Option[Class[_ <:Track]]
     
     
     ////ref loadTrack : Track

}

class DefaultTrackLoader( val trackFactory : TrackFactory, val trackProps : Map[String,String] )  extends TrackLoader with Logging {
  
  override def createClassLoader( trackPath : Path ) : ClassLoader = {
     new java.net.URLClassLoader(jarURLS(trackFactory.hdfs, trackPath  ),this.getClass.getClassLoader)
  }

  def loadTrackClass(trackClassName: String, trackClassloader: ClassLoader): Option[Class[_ <: Track]] = {
    try {

      //// Accessing object instances 
      ///val scalaName = if (trackClassName endsWith "$") trackClassName else (trackClassName + "$")
      //// XXX allow scala companion objects 
      val scalaName = trackClassName

      val scalaClass = trackClassloader.loadClass(scalaName)
      info(" Scala Class is " + scalaClass.getCanonicalName())

      info(" Scala Parent class = " + scalaClass.getSuperclass() + " :: " + scalaClass.getSuperclass.getCanonicalName())

      val t1Class = classOf[Track]
      info(" Track class = " + t1Class + " :: " + t1Class.getCanonicalName())

      val tClass = scalaClass.asSubclass(classOf[Track])
      val cons = tClass.getDeclaredConstructors();
      cons.foreach(_.setAccessible(true));

      info(" Track Scala Class is " + tClass + " :: " + tClass.getCanonicalName())
      Some(tClass)
    } catch {
      case e: Throwable =>
        /// XXX Case match any catch all throwable
        e.printStackTrace(System.out)
        error(" Could not find Track class ", e)
        None
    }

  }
        
   def jarURLS(trackFS : FileSystem, jarPath : Path ) : Array[java.net.URL] = {
     if( trackFS.isDirectory( jarPath) ) {
       trackFS.listFiles( jarPath).filter( _.path.toString.endsWith(".jar")).map( _.path.toUri.toURL).toArray
     } else {
        val hdfsUri = jarPath.toUri
        Array( hdfsUri.toURL)
     }
   }
   
}