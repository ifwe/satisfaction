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


    override def onStart(app: Application) {
        /// XXX initialize name-node, job-tracker, and metastore 
        //// with values from app.configuration 
        
        println(" Starting up Will Rogers;  I never metastore I didn't like ...")
        
        /// XXX todo -- dependency inject companion objects ????
        println(" Caching the Hive MetaStore")
        ///val initMs = hive.ms.MetaStore
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
    
    def hdfsConfig : HadoopConfiguration = {
        val conf = new HadoopConfiguration
        /// XXX 
        /// XXX JDB FIXME
        /// Avoid Hacks to point to correct filesystem
        /// XXX Clean up app  configuration 
      val testPath = System.getProperty("user.dir") + "/apps/willrogers/conf/hdfs-site.xml"
      conf.addResource( new java.io.File(testPath).toURI().toURL())
      
      
       val nameService = conf.get("dfs.nameservices")
       if(nameService != null) {
         conf.set("fs.defaultFS", s"hdfs://$nameService")
       }
      conf
    }
    val hdfsFS = Hdfs.fromConfig( hdfsConfig )
    val trackPath : Path = new Path("/user/satisfaction")
    
    println(" HDFS DFS = " + hdfsFS)
    
    
    val trackScheduler = new TrackScheduler( engine.actors.ProofEngine)
      
    implicit val trackFactory : TrackFactory = {
      ///// XXX Why doesn't implicits automatically convert???
      val hadoopWitness : Witness = Config.Configuration2Witness( hdfsConfig.asInstanceOf[HadoopConfiguration])
      var tf = new TrackFactory( hdfsFS, trackPath, Some(trackScheduler), Some(hadoopWitness))
      trackScheduler.trackFactory = tf
      tf
    }
    
    implicit val hiveConf = Config.config
    
    implicit val metaStore : MetaStore  = new MetaStore( hiveConf)
}