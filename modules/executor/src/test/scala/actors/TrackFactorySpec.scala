package com.klout.satisfaction
package executor
package track

import scalaxb._
import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class TrackFactorySpec extends Specification {

    "TrackFactorySpec" should {
        "load a Track from HDFS" in {
          val tf = new TrackFactory( new java.net.URI( "hdfs://jobs-dev-hnn"),
                 "/user/satisfaction")
          val trackDesc = TrackDescriptor("usergraph") 
          val track = tf.generateTrack(trackDesc) 
        }
        "List Tracks from HDFS" in {
          val tf = new TrackFactory( new java.net.URI( "hdfs://jobs-dev-hnn"),
                 "/user/satisfaction")
          tf.getAllTracks.foreach( td  => println(td))
 
        }
        
        "Parse a track descriptor from a known path" in {
          val tf = new TrackFactory( new java.net.URI( "hdfs://jobs-dev-hnn"),
                 "/user/satisfaction")
          
          val td = TrackDescriptor("maxwell")
         
           val path = tf.getTrackPath( td)
           println("Track Path is " + path.toUri.toString)
           
           path.toUri.toString must_== "hdfs://jobs-dev-hnn/user/satisfaction/track/maxwell/version_LATEST"
             
           val rtTd = tf.parseTrackPath( path)  
           println(" Track Descriptor is " + rtTd)
           rtTd must_== td
          
        }

       "Parse a track descriptor from a known path with user and variant" in {
          val tf = new TrackFactory( new java.net.URI( "hdfs://jobs-dev-hnn"),
                 "/user/satisfaction")
          
          val td = TrackDescriptor("maxwell", "jerome", "20.0_beta",Some("myfeature"))
         
           val path = tf.getTrackPath( td)
           println("Track Path is " + path.toUri.toString)
           
           path.toUri.toString must_== "hdfs://jobs-dev-hnn/user/satisfaction/track/maxwell/user/jerome/variant/myfeature/version_20.0_beta"
             
           val rtTd = tf.parseTrackPath( path)  
           println(" Track Descriptor is " + rtTd)
           rtTd must_== td
          
        }
       
       "Parse a track descriptor with a port number" in {
          val tf = new TrackFactory( new java.net.URI( "hdfs://jobs-dev-hnn:8020"),
                 "/user/satisfaction")
          
          val td = TrackDescriptor("maxwell", "jerome", "20.0_beta",Some("myfeature"))
         
           val path = tf.getTrackPath( td)
           println("Track Path is " + path.toUri.toString)
           
           path.toUri.toString must_== "hdfs://jobs-dev-hnn:8020/user/satisfaction/track/maxwell/user/jerome/variant/myfeature/version_20.0_beta"
             
           val rtTd = tf.parseTrackPath( path)  
           println(" Track Descriptor is " + rtTd)
           rtTd must_== td
          
        }
       
       
       "GetAllTracks" in {
          val tf = new TrackFactory( new java.net.URI( "hdfs://jobs-dev-hnn"),
                 "/user/satisfaction")
         
          val allTracks = tf.getAllTracks
          allTracks.foreach( tr => {
            println(" ALL TRACKS :: Track is " + tr )
          })
       }

    }
}