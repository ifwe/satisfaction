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
import satisfaction.fs.FileSystem
import satisfaction.hadoop.hdfs.Hdfs
import satisfaction.engine.Satisfaction
import satisfaction.engine.actors.GoalState
import satisfaction.fs.LocalFileSystem
import satisfaction.fs.Path


@RunWith(classOf[JUnitRunner])
class PartitionExistsSpec extends Specification {
    val hour = new Variable[String]("hour", classOf[String])
    val runDate = new Variable[String]("date", classOf[String])
    
    implicit val ms : MetaStore = MetaStore.default
    implicit val hdfs : FileSystem = Hdfs.default
    implicit val track : Track = Track.localTrack( "PartitionExistsTrack", 
           LocalFileSystem.relativePath( new Path(
                "modules/hadoop/src/test/resources/track/PartitionExists")))

    "PartitionExistsSpec" should {
      
      
        "Create a single partition" in {
            val vars: List[Variable[_]] = List(hour, runDate)
            
            
            val partExist = PartitionExists( HiveTable("sqoop_test", "page_view_log"))


            val witness = Witness((runDate -> "20140522"), ( hour -> "03"))

            val goalResult = Satisfaction.satisfyGoal( partExist, witness)
            println(" YYY Goal Result is " + goalResult)
            println(" YYY Goal ExecResult is " + goalResult.execResult)
            println(" YYY Goal State is " + goalResult.state)
            if( goalResult.execResult.stackTrace != null ) {
              println( " YYY " + goalResult.execResult.errorMessage)
              goalResult.execResult.stackTrace.foreach( st =>  println( " YYY " + st))
            }

            goalResult.state == GoalState.Success

        }

        "Create a FanOut set of Partitions" in {
            val vars: List[Variable[_]] = List(hour, runDate)
            
            
            val partExist = PartitionExists( HiveTable("sqoop_test", "page_view_log"))
            
            
            val allHours = ( 01 to 23 ) map ( hr => { 
                 val nuum = new java.text.DecimalFormat("00").format(hr)
                 println(" HOUR = " + nuum);
                 nuum} )
            
            
            val fanOutParts = FanOutGoal( partExist, hour, allHours)



            val witness = Witness((runDate -> "20140522"))

            val goalResult = Satisfaction.satisfyGoal( fanOutParts, witness)
            println(" YYY Goal Result is " + goalResult)
            println(" YYY Goal ExecResult is " + goalResult.execResult)
            println(" YYY Goal State is " + goalResult.state)
            if( goalResult.execResult.stackTrace != null ) {
              println( " YYY " + goalResult.execResult.errorMessage)
              goalResult.execResult.stackTrace.foreach( st =>  println( " YYY " + st))
            }

            goalResult.state == GoalState.Success

        }
      
        /**
       "Run a Hive goal" in {
            val vars: List[Variable[_]] = List(hour, runDate)
            
            
            val partExist = PartitionExists( HiveTable("sqoop_test", "page_view_log"))
            
            val fanOut = FanOutGoal( partExist, hour,  ( 0 to 23 ).map( _.toString ) )

            val witness = Witness((runDate -> "20140522"))

            val goalResult = Satisfaction.satisfyGoal( fanOut, witness)
            

            goalResult.state == GoalState.Success
        }
        * 
        */

    }
}