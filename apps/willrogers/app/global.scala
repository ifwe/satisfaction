import play.api._

import com.klout.satisfaction.executor.api._

object Global extends GlobalSettings {

    // val HdfsProjectRootPath = "/user/jerome/satisfaction/prod/projects"
    val HdfsProjectRootPath = "/satisfaction/prod/projects"

    override def onStart(app: Application) {
        Api.initProjects(HdfsProjectRootPath)
        
        println(" Starting up Will Rogers;  I never metastore I didn't like ...")
        
        /// XXX todo -- dependency inject companion objects ????
        println(" Caching the Hive MetaStore")
        ///val initMs = hive.ms.MetaStore
        println(" Starting the Akka Actors")
        val initPe = com.klout.satisfaction.executor.actors.ProofEngine
        println(" Loading all Tracks ")
        val initTf = com.klout.satisfaction.executor.track.TrackFactory.getAllTracks
        initTf.foreach( tr => { 
          try {
           println(" Track " + tr.trackName + " User " + tr.forUser + " with variant " + tr.variant + " :: Version " + tr.version)
           val loadTr = com.klout.satisfaction.executor.track.TrackFactory.getTrack(tr)
           println(" Loaded Track " + loadTr.get)
          } catch {
            case e: Throwable =>
              println(" Unable to load track " +tr + " Exc = " + e)
              e.printStackTrace
          }
        })
    }
}