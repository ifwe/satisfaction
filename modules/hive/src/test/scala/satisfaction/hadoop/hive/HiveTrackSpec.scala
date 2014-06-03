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



@RunWith(classOf[JUnitRunner])
class HiveTrackSpec extends Specification {
  
  
     object TestHiveTrack extends HiveTrack( TrackDescriptor("DauBackfill")) {
       
       setTrackPath(Path("/user/satisfaction/track/DauBackfill/version_0.2"))
       
       this.setTrackProperties( Witness( ( VariableAssignment(Variable("dauDB") ,"sqoop_test" ))) )
       
       val DauByPlatformTable = new HiveTable( "sqoop_test", "dau_by_platform_sketch") {
          override def exists( w: Witness )  = { false }
       }
       
         
        val DauByPlatformSketch = HiveGoal(
           "DAU By Platform", /// Name of our Goal
           "dau_by_platform_sketch.hql", /// Hive Resource file
           DauByPlatformTable).declareTopLevel   /// Our table outpu
       
     }
    

    "HiveTrackSpec" should {
        "Run A Query" in {
           val dt = Variable("dt")
           val hour = Variable("hour")
           
           val witness = Witness( VariableAssignment( dt ,"20140522"), VariableAssignment( hour, "01"))
           
           
           val result = Satisfaction.satisfyGoal( TestHiveTrack.DauByPlatformSketch, witness)
           
           
           result.state must_== GoalState.Success
        }

    }
}