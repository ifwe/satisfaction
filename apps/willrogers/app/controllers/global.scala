package willrogers;

import play.api._
import satisfaction._
import satisfaction.track.TrackFactory
import satisfaction.track.TrackScheduler
import satisfaction.hadoop.hdfs.Hdfs
import satisfaction.hadoop.hive.ms._
import satisfaction.hadoop.Config
import satisfaction.fs.Path
import satisfaction.hadoop.Config
import satisfaction.track.JDBCSlickTrackHistory
import satisfaction.engine.actors.ProofEngine
import play.api.mvc.RequestHeader
import play.api.mvc.Results._
import scala.concurrent.Future
import satisfaction.track.TrackHistory



object Global extends play.api.GlobalSettings {

    implicit val hiveConf = Config.config
    implicit val metaStore : MetaStore  = new MetaStore()( hiveConf)

    lazy val hdfsFS = Hdfs.fromConfig( hiveConf )

    var trackPath : Path = new Path("/user/satisfation")


    override def onStart(app: Application) {
        super.onStart( app)
        /// XXX initialize name-node, job-tracker, and metastore 
        //// with values from app.configuration 
        
        Logger.info(" Starting up Will Rogers;  I never metastore I didn't like ...")
        
        Logger.info(" Starting the Akka Actors")
        
        val initPe = proofEngine

        trackPath = Path(app.configuration.getString("satisfaction.track.path").getOrElse("/user/satisfaction"))
        Logger.info(s" Using TrackPath $trackPath ")
        val hadoopWitness : Witness = Config.Configuration2Witness( hiveConf)
        Logger.info("XXXXXXXXXXXX Creating GLOBAL TrackFactory YYYYYYYYYYYY")
        var tf = new TrackFactory( hdfsFS, trackPath, Some(trackScheduler), Some(hadoopWitness))
        trackFactory =tf
        trackScheduler.trackFactory = tf
        trackFactory.initializeAllTracks

    }
    
    //// XXX Add Driver info ...
    
    lazy val trackHistory : TrackHistory = JDBCSlickTrackHistory
    lazy val proofEngine = new ProofEngine(Some(trackHistory))
    
    lazy val trackScheduler = new TrackScheduler(proofEngine)
    
    var trackFactory : TrackFactory = null
    
    override def onError(request: RequestHeader, ex: Throwable) = {
       Future.successful(
         InternalServerError( views.html.errorPage(ex) )
       )
    }  
    
}