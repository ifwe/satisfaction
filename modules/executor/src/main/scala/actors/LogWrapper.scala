package com.klout.satisfaction
package executor
package actors

import java.io._
import scala.Console
import hive.ms.Hdfs
import org.apache.hadoop.fs.Path

/**
 *  Divert all output from STDOUT and STDERR to a defined log file
 *  
 */
case class LogWrapper[T]( track : Track, goal : Goal, witness : Witness) {

  
  def log( functor :  () => T  ) : Option[T] = {
     val currOut = Console.out
     val currErr = Console.err
     val outStream = getLoggingOutput
     try {
         Console.setOut(outStream)
         Console.setErr(outStream)
       
         val result : T =  functor()
         
         Some(result)
     } catch {
        case t: Throwable =>
        println(t, "Unexpected Error while running job")
        t.printStackTrace(currErr)
        t.printStackTrace(new java.io.PrintWriter(outStream))
       
        None
    } finally {
      outStream.flush()
      outStream.close()
      LogWrapper.uploadToHdfs(track, goal, witness)
      
      Console.setOut(currOut)
      Console.setOut(currErr)
    }
  }

  def getLoggingOutput: OutputStream = {
     new FileOutputStream( LogWrapper.logPathForGoalWitness( track, goal, witness) )
  }
  
  def getHdfsLogPath : String  = {
     LogWrapper.hdfsPathForGoalWitness( track, goal, witness)
  }

}

object LogWrapper {
  
    val rootDirectory = new File(System.getProperty("user.dir") + "/logs")
    /// Dependency injection
    val hdfsRootDirectory = "/user/satisfaction/logs"
    val hdfs : Hdfs = Hdfs
    
    
    def pathString( str : String ) : String = {
      str.replace(" ","_").replace("=>","@").replace("(","_").replace(")","_")
    }
    
    def logPathForGoalWitness( track: Track, goal : Goal, witness : Witness ) : File = {
        new File(rootedPathForGoalWitness( LogWrapper.rootDirectory.getPath ,track, goal, witness))
    }
    
    def hdfsPathForGoalWitness( track: Track, goal : Goal, witness : Witness ) : String = {
        rootedPathForGoalWitness( hdfsRootDirectory ,track, goal, witness)
    }
    
    def rootedPathForGoalWitness(root: String, track: Track, goal : Goal, witness : Witness ) : String = {
        val goalFile = new File( root + "/" + pathString(track.name) + "/" + pathString(goal.name) )
        goalFile.mkdirs
        goalFile.getPath() +  "/" + pathString(witness.substitution.toString ) 
    }
    
    
    def uploadToHdfs( track : Track, goal : Goal, witness : Witness ) = {
      try {
        val localPath = logPathForGoalWitness( track, goal, witness)
        val destPath = hdfsPathForGoalWitness( track, goal, witness)
        hdfs.fs.copyFromLocalFile( false, true, new Path( localPath.getPath ), new Path( destPath))
      } catch {
        case unexpected : Throwable =>
          System.out.println(" Unexpected error copying logs ot HDFS" + unexpected)
          unexpected.printStackTrace
      }
    }
    
    /// Parse the path, in order to determine the goals and Witness
    //// XXX Change to tuple2[String,Witness] and add  Trackname 
    def getGoalFromPath( path : File ) : Tuple3[String,String,String] = {
       if( path.toString.startsWith( rootDirectory.toString )) {
           val splitArr = path.toString.substring( rootDirectory.toString.length).split("/")
           val  gw = new Tuple3[String,String,String]( splitArr(0) , splitArr(1), splitArr(2))
           
           gw
       } else {
         null
       }
    }
    
   def getLogPathsForGoal( trackName : String, goalName : String )  : Set[String] = {
     val goalPath = new File( rootDirectory + "/" +pathString( trackName) + "/" + pathString( goalName) )
     println(" Goal Path is " + goalPath.getPath)
     ///val goalTuple = get
     
     ///goalPath.listFiles.map( getGoalFromPath( _ )).map( _._2).toSet
     
     goalPath.listFiles.map( _.getPath ).toSet
   }
    
}