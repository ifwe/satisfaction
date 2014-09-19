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
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import satisfaction.track.TrackFactory.TracksUnavailableException



object Global extends play.api.GlobalSettings {

    implicit lazy val hiveConf = Config.config
    implicit lazy val metaStore : MetaStore  = new MetaStore()( hiveConf)

    lazy val hdfsFS = Hdfs.fromConfig( hiveConf )

    var trackPath : Path = new Path("/user/satisfation")


    override def onStart(app: Application) {
        super.onStart( app)
        /// XXX initialize name-node, job-tracker, and metastore 
        //// with values from app.configuration 
        
        Logger.info(" Starting up Will Rogers;  I never metastore I didn't like ...")
        
        Logger.info(" Starting the Akka Actors")
        
        val initPe = proofEngine


        trackPath = trackPath( app.configuration)
        Logger.info(s" Using TrackPath $trackPath ")
        

        
        
        val hadoopWitness : Witness = Config.Configuration2Witness( hiveConf)
        ///  Looking for failover values
        Logger.info(" HA Failover Provider is :: ")
        hadoopWitness.assignments.filter( _.variable.name.startsWith("dfs.client.failover.proxy.provider")).foreach { ass => {
              Logger.info(s"   ${ass.variable.name} = ${ass.value} ")
           }
        }
       
        Logger.info(s"Hadoop Configuration is $hiveConf")

        /**
        val buffer = new ByteArrayOutputStream
        hiveConf.writeXml( new DataOutputStream(buffer))
        Logger.info( buffer.toString) 
        * 
        */
        
        
        Logger.info("XXXXXXXXXXXX Creating GLOBAL TrackFactory YYYYYYYYYYYY")
        var tf = new TrackFactory( hdfsFS, trackPath, Some(trackScheduler), Some(hadoopWitness))
        trackFactory =tf
        trackScheduler.trackFactory = tf
        try {
          trackFactory.initializeAllTracks
          Logger.info(" Tracks initialized.")
        } catch {
          case noTracks : TracksUnavailableException =>
            Logger.warn(s" Unable to load tracks ${noTracks.getLocalizedMessage()} ")
          case unexpected : Throwable => throw unexpected
        }

    }
    
    def trackPath( playConfig  : Configuration) : Path = {
        playConfig.getString("satisfaction.track.path") match {
          case Some(trackPath) => Path(trackPath)
          case None => {
             val user = System.getProperty("user.name")
             user match {
               case "satisfaction" => Path("/user/satisfaction")
               case "root" => Path("/user/satisfaction")
               case _ => Path(s"/user/${user}/satisfaction")
             }
          }
        }
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