package willrogers

import play.api._
import com.klout.satisfaction._
import com.klout.satisfaction.track.TrackFactory
import com.klout.satisfaction.track.TrackScheduler
import com.klout.satisfaction.hadoop.hdfs.Hdfs
import com.klout.satisfaction.hadoop.hive.ms._
import com.klout.satisfaction.hadoop.Config
import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }
import com.klout.satisfaction.fs.Path

import com.klout.satisfaction.hadoop.Config

object Global extends GlobalSettings {

    implicit val hiveConf = Config.config
    implicit val metaStore : MetaStore  = new MetaStore( hiveConf)

    lazy val hdfsFS = Hdfs.fromConfig( hiveConf )
    lazy val trackPath : Path = new Path("/user/satisfaction") /// XXX Get From appconfig
    


    override def onStart(app: Application) {
        /// XXX initialize name-node, job-tracker, and metastore 
        //// with values from app.configuration 
        
        println(" Starting up Will Rogers;  I never metastore I didn't like ...")
        
        println(" Starting the Akka Actors")
        val initPe = engine.actors.ProofEngine
        println(" Loading all Tracks ")
            /// 
        val initTf = trackFactory.getAllTracks
        initTf.foreach( tr => { 
          try {
           println(" Track " + tr.trackName + " User " + tr.forUser + " with variant " + tr.variant + " :: Version " + tr.version)
           val loadTr = trackFactory.getTrack(tr)
           println(" Loaded Track " + loadTr.get)
          } catch {
            case e: Throwable =>
              println(" Unable to load track " +tr + " Exc = " + e)
              e.printStackTrace
          }
        })
    }
    
    
    val trackScheduler = new TrackScheduler( engine.actors.ProofEngine)
      
    implicit val trackFactory : TrackFactory = {
      ///// XXX Why doesn't implicits automatically convert???
      try {
      val hadoopWitness : Witness = Config.Configuration2Witness( hiveConf)
      var tf = new TrackFactory( hdfsFS, trackPath, Some(trackScheduler), Some(hadoopWitness))
      trackScheduler.trackFactory = tf
      tf
      } catch {
        case unexpected : Throwable => 
           unexpected.printStackTrace(System.out) 
           throw unexpected
      }
    }
    

}