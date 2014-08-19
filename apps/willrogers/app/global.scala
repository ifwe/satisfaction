package willrogers

import play.api._
import scala.slick.driver.H2Driver.simple._
import play.api.GlobalSettings
import scala.slick.driver.H2Driver.simple._
import satisfaction._
import satisfaction.track.TrackFactory
import satisfaction.track.TrackScheduler
import satisfaction.hadoop.hdfs.Hdfs
import satisfaction.hadoop.hive.ms._
import satisfaction.hadoop.Config
import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }
import satisfaction.fs.Path
import satisfaction.hadoop.Config
import satisfaction.track.TrackHistory
import satisfaction.track.JDBCSlickTrackHistory
import satisfaction.engine.actors.ProofEngine


object Global extends GlobalSettings {

    implicit val hiveConf = Config.config
    implicit val metaStore : MetaStore  = new MetaStore()( hiveConf)

    lazy val hdfsFS = Hdfs.fromConfig( hiveConf )

    ////lazy val trackPath : Path = new Path("/user/jerome/satisfaction") /// XXX Get From appconfig
    lazy val trackPath : Path = new Path(
            configuration.getString("satisfaction.path").getOrElse(
           "/user/satisfaction")) 


    override def onStart(app: Application) {
        /// XXX initialize name-node, job-tracker, and metastore 
        //// with values from app.configuration 
        
        println(" Starting up Will Rogers;  I never metastore I didn't like ...")
        
        println(" Starting the Akka Actors")
        
        val initPe = proofEngine
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
       // lazy val database = Database.forDataSource(DB.getDataSource())

    }
    
    //// XXX Add Driver info ...
    
    val trackHistory : TrackHistory = JDBCSlickTrackHistory
    val proofEngine = new ProofEngine(Some(trackHistory))
    
    val trackScheduler = new TrackScheduler(proofEngine)
      
    implicit val trackFactory : TrackFactory = {
      ///// XXX Why doesn't implicits automatically convert???
      try {
      val hadoopWitness : Witness = Config.Configuration2Witness( hiveConf)
      println("XXXXXXXXXXXX Creating GLOBAL TrackFactory YYYYYYYYYYYY")
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