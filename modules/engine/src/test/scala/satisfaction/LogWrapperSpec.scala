package com.klout
package satisfaction
package engine
package actors


import org.specs2.mutable._
import satisfaction.fs.LocalFileSystem
import org.joda.time.DateTime

class LogWrapperSpec extends Specification {
  "The 'Cheese' string" should {
    "contain 6 characters" in {
      "Cheese" must have size(6)
    }
    
  }
  
  "LogWrapper" should {
      "Produces proper paths" in {
         val pathStr = "hdfs://hdp2/data/ramblas/event_log/${event_type}/${dt}/${hour}" 
           
           
         val escapedStr = LogWrapper.pathString( pathStr)   
         println("Escaped is " + escapedStr)
         
         /// Check to see if we can create a file 
         val checkFile = new java.io.File("/tmp/" + escapedStr)
         checkFile.createNewFile
        
         checkFile.delete
      } 

      
      class MockEvidence extends DataOutput with DataInstance {
          var doesExist : Boolean = false 
          val dt = DateTime.now
          
          
          override def exists( w: Witness) : Boolean = {
            doesExist
          }
          
          override def exists : Boolean = { doesExist }
        
          override def variables = List( Variable("dt"), Variable("hour"))
        
            
          override def getDataInstance(witness: Witness): Option[DataInstance] = Some(this)
          
          override def created: DateTime = dt
          override def lastAccessed: DateTime = dt
          override def size  = 0

      }
      
      
      "Produce a witnesspath" in {
         implicit val hdfs = LocalFileSystem
         
         val evidence = new MockEvidence

         implicit val track : Track = new Track(TrackDescriptor("DataDependency")) {

           def dep =  DataDependency( evidence  )
           override def init = {
             dep.declareTopLevel
           }
           

         }
         track.init
         
         
         val witness = Witness( Variable("dt") -> "20140715", Variable("hour") -> "23" )
         val goal = track.topLevelGoals.head

         val wrapper = new LogWrapper[Unit]( track, goal, witness)
         
         
         wrapper.info("What is going on Dude ???")
         wrapper.info("Want to see the write output ")
         
         wrapper.error(" Something awful happended ")
         
         
         
         
         wrapper.close
         
         LogWrapper.uploadToHdfs(track, goal, witness)
           
         val hdfsPath = wrapper.hdfsLogPath
         
         println(s" HDFS PATH is $hdfsPath ")
         
         true
      } 
  }
}