package satisfaction
package track

import org.specs2.mutable._
import satisfaction.Witness
import satisfaction._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import io._
import satisfaction.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => ApachePath}
import satisfaction.hadoop.hdfs.Hdfs

import satisfaction.track._

/**
 *  Test that TrackFactory works with an HdfsFileSystem
 *  
 */

@RunWith(classOf[JUnitRunner])
class HdfsTrackFactorySpec extends Specification {
  

    def hdfs = Hdfs.fromConfig(clientConfig)
  
  
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

  
    "HdfsTrackFactory" should {
      
        "List Tracks" in {
           val tf = new TrackFactory(hdfs) 
           
           val allTracks = tf.getAllTracks
           System.out.println(" Number of tracks is "+ allTracks.size)
           allTracks.foreach( td => {
                System.out.println( td.trackName + " :: " + td.version) 
           })
          
        }
        
        /**
        
        "Load A Track" in {
           val tf = new TrackFactory(hdfs) 
          
           val td = new TrackDescriptor(trackName="Sample", forUser="Sample", version="2.3")
           
           val track = tf.getTrack( td)
           
           System.out.println(" TRACK IS " + track)
        }
        
        "Get A Track Path" in {
           val tf = new TrackFactory(hdfs) 
          
           val td = new TrackDescriptor(trackName="Sample", forUser="Sample", version="2.3")
           
           val tp = tf.getTrackPath(td)
           
           System.out.println(" TRACKPath IS " + tp)
        }
        
        "Load track Class" in {
           val tf = new TrackFactory(hdfs) 
          
           val td = new TrackDescriptor(trackName="Sample", forUser="Sample", version="2.3")
           
           val tp = tf.getTrackPath(td)
           val jarPath : Path = tp / "lib" / "satisfy-simple_2.10-2.3.jar"
           ///val jarPath : Path = tp / "lib"
           
           val trackClassName = "com.klout.satisfy.track.simple.WorkAroundTrack" 
             
           val track = tf.loadTrackClass(jarPath, trackClassName)
           System.out.println( " Track is " + track)
        }
        * 
        */
          
    }

}