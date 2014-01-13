package com.klout

import play.api._

import satisfaction._

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
        println(" Loading all Tracks "
            /// 
        val initTf = com.klout.satisfaction.engine.track.TrackFactory.getAllTracks
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