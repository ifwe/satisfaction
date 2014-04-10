package com.klout
package satisfaction
package engine
package actors

import java.io._
import scala.Console
///import hive.ms.Hdfs
///import org.apache.hadoop.fs.Path
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 *  Divert all output from STDOUT and STDERR to a defined log file
 *  
 */
case class LogWrapper[T]( track : Track, goal : Goal, witness : Witness) {

  /**
   * XXX Use satisfaction.fs abstraction instead of files
   */
   
  def log( functor :  () => T  ) : Try[T] = {
     val currOut = Console.out
     val currErr = Console.err
     val outStream = getLoggingOutput
     try {
         Console.setOut(outStream)
         Console.setErr(outStream)
       
         val result : T =  functor()
         
         Success(result)
     } catch {
        case t: Throwable =>
        println(t, "Unexpected Error while running job")
        t.printStackTrace(currErr)
        t.printStackTrace(new java.io.PrintWriter(outStream))
       
        Failure( t)
    } finally {
      outStream.flush()
      outStream.close()
      LogWrapper.uploadToHdfs(track, goal, witness)
      
      Console.setOut(currOut)
      Console.setOut(currErr)
    }
  }

  def getLoggingOutput: OutputStream = {
     new FileOutputStream( LogWrapper.logPathForGoalWitness( track.descriptor, goal.name, witness) )
  }
  
  def getHdfsLogPath : String  = {
     LogWrapper.hdfsPathForGoalWitness( track.descriptor, goal.name, witness)
  }
  
  
  /**
   *  Allow the log output to be streamed directly,
   *    So that it can be seen through some UI  
   */
  def streamLogs : InputStream = {
     new FileInputStream( LogWrapper.logPathForGoalWitness( track.descriptor, goal.name, witness) )
  }

}

object LogWrapper {
  
    val rootDirectory = new File(System.getProperty("user.dir") + "/logs")
    /// Dependency injection
    val hdfsRootDirectory = "/user/satisfaction/logs"
    ///val hdfs :  FileSystem  = null
    
    
    def pathString( str : String ) : String = {
      str.replace(" ","_").replace("=>","@").replace("(","_").replace(")","_")
    }
    
    def logPathForGoalWitness( track: TrackDescriptor, goalName : String, witness : Witness ) : File = {
        new File(rootedPathForGoalWitness( LogWrapper.rootDirectory.getPath ,track, goalName, witness))
    }
    
    def hdfsPathForGoalWitness( track: TrackDescriptor, goalName : String, witness : Witness ) : String = {
        rootedPathForGoalWitness( hdfsRootDirectory ,track, goalName, witness)
    }
    
    def rootedPathForGoalWitness(root: String, track: TrackDescriptor, goalName : String, witness : Witness ) : String = {
        val goalFile = new File( root + "/" + pathString(track.trackName) + "/" + pathString(goalName) )
        goalFile.mkdirs
        goalFile.getPath() +  "/" + pathString(witness.substitution.toString ) 
    }
    
    
    def uploadToHdfs( track : Track, goal : Goal, witness : Witness ) = {
      try {
        val localPath = logPathForGoalWitness( track.descriptor, goal.name, witness)
        val destPath = hdfsPathForGoalWitness( track.descriptor, goal.name, witness)
        ///hdfs.fs.copyFromLocalFile( false, true, new Path( localPath.getPath ), new Path( destPath))
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
    
   def getGoalLogMap( trackName : String ) : Map[String,List[String]] = {
     val trackPath = new File( rootDirectory + "/"  + pathString( trackName))
     if( trackPath.exists) {
       trackPath.listFiles.map(  _.getName).map( gname => { 
         ( gname, getLogPathsForGoal(trackName, gname))
       } ) toMap
       
     } else {
       Map.empty
     }
     
   }
    
   def getLogPathsForGoal( trackName : String, goalName : String )  : List[String] = {
     //// XXX Use FileSystem abstraction 
     /// and paths 
     val goalPath = new File( rootDirectory + "/" +pathString( trackName) + "/" + pathString( goalName) )
     ///println(" Goal Path is " + goalPath.getPath)
     if( !goalPath.exists) {
       println(s"Creating Log directory $goalPath")
       goalPath.mkdirs
     }
     ///val goalTuple = get
     
     ///goalPath.listFiles.map( getGoalFromPath( _ )).map( _._2).toSet
     
     goalPath.listFiles.map( _.getPath ).toList
   }
   
   /**
    *   Parse the _dt_@_20140127:_network_abbr_@_li_ 
    *     syntax used to generate the path
    */
   def getWitnessFromLogPath( logPath : String ) : Witness = {
     val kvAss = logPath.split(";") map ( _.split("@") ) map ( kvArr => 
        {  VariableAssignment[String](Variable( kvArr(0).substring(1, kvArr(0).length -1)), 
             kvArr(1).substring(1, kvArr(1).length -1) ) } ) 
     
     Witness( kvAss:_*)      
   }
    
}