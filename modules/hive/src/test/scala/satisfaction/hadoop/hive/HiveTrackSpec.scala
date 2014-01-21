package com.klout
package satisfaction
package hadoop
package hive

import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level
import hadoop.hive.ms._
import satisfaction.fs._
import satisfaction.hadoop.hdfs.Hdfs
import satisfaction.engine.Satisfaction
import satisfaction.engine.actors.GoalState
import satisfaction.track._

/**
 *  Test that Hive Goals work with HiveGoals which have been loaded from
 *    TrackFactory 
 */


object MockTrackFactory   extends TrackFactory(
   new LocalFileSystem( System.getProperty("user.dir") + "/src/test/resources") ) {
  
}

@RunWith(classOf[JUnitRunner])
class HiveTrackSpec extends Specification {
    val NetworkAbbr = new Variable[String]("network_abbr", classOf[String])
    val DoDistcp = new Variable[Boolean]("doDistcp", classOf[Boolean])
    val runDate = new Variable[String]("dt", classOf[String])
    
    implicit val ms : MetaStore = MetaStore( new java.net.URI("thrift://jobs-dev-sched2:9085") )
    implicit val hdfs : FileSystem = new Hdfs("hdfs://jobs-dev-hnn") 

    "HiveGoalSpec" should {
       "Get Track" in {
           val trackFactory = new TrackFactory(hdfs)
           val allTracks = trackFactory.getAllTracks
           System.out.println(" Number of tracks is " + allTracks.length)
           allTracks.foreach( td => { println(s"Track is $td ") } )
           
       }

    }
}