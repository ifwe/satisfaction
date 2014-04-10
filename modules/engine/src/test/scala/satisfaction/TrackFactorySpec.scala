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
  
        val mockFS = new LocalFileSystem
        val resourcePath = LocalFileSystem.currentDirectory / "modules" / "engine" / "src" /
            "test" / "resources";
   
        val mockTrackFactory  = new TrackFactory( mockFS, resourcePath  / "user" / "satisfaction")

        
      "TrackFactorySpec" should {
          
         
      
        "Be able to list tracks" in {
            mockFS.listFiles( resourcePath / "/user/satisfaction/track").foreach( println(_))
            println(s" Resoure Path is $resourcePath" )
            val allTracks = mockTrackFactory.getAllTracks 
            allTracks.foreach(  println( _ ) )
            
            println(" AllTracks Size = " + allTracks.size)
            
            
            ! (allTracks must haveSize(0))
        }
        /**

        "Be able to load Track object " in {
            val tf = mockTrackFactory
            val td = TrackDescriptor("Simple")
          
            val track = tf.getTrack( td)
          
            true
        }
        
        "Be able to inialize TrackClass" in {
            val tf = mockTrackFactory
            val jarPath = new Path("simple/lib")
            val trackClassName= "com.klout.satisfy.track.simple"
            val trackClass =tf.loadTrackClass(jarPath, trackClassName)
            
            println(" TrackClass is " + trackClass)
          
     
            trackClass must_!= null
            
            trackClass must_!= None
        }
        * **
        */

     }
}