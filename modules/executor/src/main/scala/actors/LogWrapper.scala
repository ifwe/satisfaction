package com.klout.satisfaction
package executor
package actors

import java.io._
import scala.Console

/**
 *  Divert all output from STDOUT and STDERR to a defined log file
 *  
 */
case class LogWrapper[T]( goal : Goal, witness : Witness) {

  
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
      Console.setOut(currOut)
      Console.setOut(currErr)
    }
  }

  def getLoggingOutput: OutputStream = {
     new FileOutputStream( LogWrapper.logPathForGoal( goal.name, witness))
  }

}

object LogWrapper {
  
    val rootDirectory = new File(System.getProperty("user.dir") + "/logs")
    
    
    def pathString( str : String ) : String = {
      str.replace(" ","_").replace("=>",":").replace("(","_").replace(")","_")
    }
    
    def logPathForGoal( goal : String, witness : Witness ) : File = {
        val goalFile = new File( LogWrapper.rootDirectory.getPath + "/" + pathString(goal) )
        goalFile.mkdirs
        new File(goalFile.getPath() +  "/" + pathString(witness.substitution.toString ) )
    }
    
    /// Parse the path, in order to determine the goals and Witness
    //// XXX Change to tuple2[String,Witness] and add  Trackname 
    def getGoalFromPath( path : File ) : Tuple2[String,String] = {
       if( path.toString.startsWith( rootDirectory.toString )) {
           val splitArr = path.toString.substring( rootDirectory.toString.length).split("/")
           val  gw = new Tuple2[String,String]( splitArr(0) , splitArr(1))
           
           gw
       } else {
         null
       }
    }
    
   def getLogPathsForGoal( goalName : String )  : Set[String] = {
     val goalPath = new File( rootDirectory + "/" +pathString( goalName) )
     goalPath.listFiles.map( getGoalFromPath( _ )).map( _._2).toSet
   }
    
}