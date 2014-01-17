package com.klout
package satisfaction
package track

import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import scala.util.Success
import scala.util.Failure
import com.klout.satisfaction.engine._
import fs._

/**
 *  Test that we're able 
 */
class TrackFactorySpec extends Specification {
  
   val mockFS = new LocalFileSystem( System.getProperty("user.dir") + "/src/test/resources" )
   val mockTrackFactory  = new TrackFactory( mockFS)

    "TrackFactorySpec" should {
      
        "Be able to list tracks" in {
            val allTracks = mockTrackFactory.getAllTracks 
            allTracks.foreach(  println( _ ) )
              
            
            
            ! (allTracks must haveSize(0))
        }


    }
}