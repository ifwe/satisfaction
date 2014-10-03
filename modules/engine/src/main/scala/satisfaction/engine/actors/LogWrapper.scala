package satisfaction
package engine
package actors

import scala.Console
import fs._
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.io.OutputStream
import java.io.InputStream
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.{ Logger => LogbackLogger }
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.ConsoleAppender
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import ch.qos.logback.core.AppenderBase
import java.io.PrintWriter
import org.joda.time.DateTime


/**
 *  Divert all output from STDOUT and STDERR to a defined log file
 *  
 */
case class LogWrapper[T]( track : Track, goal : Goal, witness : Witness)  {
  private var _outStream : OutputStream = null

  def outStream = { 
     if( _outStream == null ) {
       _outStream = loggingOutput
    }
     _outStream
  }
  def resetOutStream = { _outStream = null } 
  
  lazy val printWriter = new PrintWriter(outStream)

  def log( functor :  () => T  ) : Try[T] = {
     resetOutStream
     val currOut = Console.out
     val currErr = Console.err
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
      close()
      
      Console.setOut(currOut)
      Console.setOut(currErr)
    }
  }
 
  def info( st : String) = {
    //// XXX Use standard log formatting
    ///  include timestampe and thread information 
    printWriter.println(s"LOGWRAPPER ${DateTime.now} $st")
  }

  def error( st : String) = {
    //// XXX Use standard log formatting
    /// 
    printWriter.println(s"ERROR LOGWRAPPER ${DateTime.now} $st")

  }
  
  def error( st : String, t:Throwable) = {
    //// XXX Use standard log formatting
    /// 
    printWriter.println(s"ERROR LOGWRAPPER ${DateTime.now} $st")

  }
  
  def debug( st : String) = {
    //// XXX Use standard log formatting
    /// 
    printWriter.println(s"DEBUG LOGWRAPPER ${DateTime.now} $st")
  }

  def warn( st : String) = {
    //// XXX Use standard log formatting
    /// 
    printWriter.println(s"WARN LOGWRAPPER ${DateTime.now} $st")
  }
  
  
  def close() {
      outStream.flush()
      outStream.close()
      LogWrapper.uploadToHdfs(track, goal, witness)
  }

  def loggingOutput: OutputStream = {
    val checkPath =  LogWrapper.logPathForGoalWitness( track.descriptor, goal.name, witness) 
    if( LogWrapper.localFS.exists( checkPath)) {
       val prevAttempts = LogWrapper.numAttemptsForGoalWitness( track.descriptor, goal.name, witness ) 
       LogWrapper.localFS.create( LogWrapper.logPathForGoalWitnessAndAttempt( track.descriptor, goal.name, witness, prevAttempts) )
    } else {
       LogWrapper.localFS.create( LogWrapper.logPathForGoalWitness( track.descriptor, goal.name, witness) )
    }
  }
  
  def hdfsLogPath : Path  = {
     LogWrapper.hdfsPathForGoalWitness( track.descriptor, goal.name, witness)
  }
  
  
  /**
   *  Allow the log output to be streamed directly,
   *    So that it can be seen through some UI  
   */
  def streamLogs : InputStream = {
    LogWrapper.localFS.open(LogWrapper.logPathForGoalWitness( track.descriptor, goal.name, witness) )
  }

}

object LogWrapper {
    val localFS : FileSystem = LocalFileSystem
  
    val rootDirectory : Path = Path(System.getProperty("user.dir")) / "logs"
    val hdfsRootDirectory : Path = Path("/user/satisfaction/logs")
    
    
    def pathString( str : String ) : String = {
      str.replace(" ","_").
      	replace("+","_").
      	replace("=>","@").
      	replace("(","_").
      	replace(")","_").
      	replace("/","_sl_").
      	replace(":","_colon_").
      	replace("{","_c_").
      	replace("}","_c_").
      	replace("}","c").
      	replace("$","dlr")
    }
    
    def logPathForGoalWitness( track: TrackDescriptor, goalName : String, witness : Witness ) : Path = {
        rootedPathForGoalWitness( LogWrapper.rootDirectory ,track, goalName, witness)
    }

    def logPathForGoalWitnessAndAttempt( track: TrackDescriptor, goalName : String, witness : Witness, attemptNum: Int ) : Path = {
      if(attemptNum == 0 )
        Path(s"${rootedPathForGoalWitness( LogWrapper.rootDirectory ,track, goalName, witness)}" )
      else
        Path(s"${rootedPathForGoalWitness( LogWrapper.rootDirectory ,track, goalName, witness)}__ATTEMPT_${attemptNum}" )
    }


    def numAttemptsForGoalWitness( track: TrackDescriptor, goalName : String, witness : Witness ) : Int = {
    	
       val checkPath = logPathForGoalWitness(track,goalName, witness)
       	localFS.listFiles(checkPath.parent).foreach(file => println("  file:" + file))
       localFS.listFiles(checkPath.parent).count( _.path.name.startsWith( checkPath.name) )
    }
    
    def hdfsPathForGoalWitness( track: TrackDescriptor, goalName : String, witness : Witness ) : Path = {
        rootedPathForGoalWitness( hdfsRootDirectory ,track, goalName, witness)
    }
    
    def rootedPathForGoalWitness(root: Path, track: TrackDescriptor, goalName : String, witness : Witness ) : Path = {
        val goalFile =  root / pathString(track.trackName) / pathString(goalName) 
        localFS.mkdirs( goalFile)
        goalFile / pathString(witness.toString ) 
    }
    
    
    def uploadToHdfs( track : Track, goal : Goal, witness : Witness ) = {
      try {
        val localPath : Path = logPathForGoalWitness( track.descriptor, goal.name, witness)
        val destPath : Path = hdfsPathForGoalWitness( track.descriptor, goal.name, witness)
        LocalFileSystem.copyToFileSystem( track.hdfs, localPath, destPath) 
      } catch {
        case unexpected : Throwable =>
          System.out.println(" Unexpected error copying logs ot HDFS" + unexpected)
          unexpected.printStackTrace
      }
    }
    
    /// Parse the path, in order to determine the goals and Witness
    //// XXX Change to tuple2[String,Witness] and add  Trackname 
    def getGoalFromPath( path : Path ) : Tuple3[String,String,String] = {
       if( path.toString.startsWith( rootDirectory.toString )) {
           val splitArr = path.toString.substring( rootDirectory.toString.length).split("/")
           val  gw = new Tuple3[String,String,String]( splitArr(0) , splitArr(1), splitArr(2))
           
           gw
       } else {
         null
       }
    }
    
   def getGoalLogMap( trackName : String ) : Map[String,List[String]] = {
     val trackPath =  rootDirectory / pathString( trackName)
     if( localFS.exists( trackPath) ) {
       localFS.listFiles( trackPath).map(  _.path.name).map( gname => { 
         ( gname, getLogPathsForGoal(trackName, gname).map( _.path.name ).toList )
       } ) toMap
       
     } else {
       Map.empty
     }
     
   }
   
   def getGoalLogName( trackName : String ) : List[String] = {
     val trackPath =  rootDirectory / pathString( trackName)
     if( localFS.exists( trackPath) ) {
       localFS.listFiles(trackPath).map( _.path.name).toList
     } else {
       Nil
     }
   }
   
   def getGoalLogRuns (trackName : String, goalName : String, pageNumber : Option[Int] = None) : List[String] = {

     val batchLength = 100
     val trackPath = rootDirectory / pathString(trackName)
     if (localFS.exists(trackPath)) {
       val returnList = getLogPathsForGoal(trackName, goalName).map(_.path.name).toList
       returnList
     } else {
       Nil
     }
   }

    
   def getLogPathsForGoal( trackName : String, goalName : String )  : Seq[FileStatus] = {
     val goalPath : Path =  rootDirectory / pathString( trackName) / pathString( goalName) 
     
     localFS.listFiles( goalPath)
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
 
   class ScalaConsoleAppender[E] extends AppenderBase[E] {
     
       def append( event : E )  {
         System.out.println(s" Sys.out SATISFACTION :: ${event.toString} ")
         Console.println(s"Console.out SATISFACTION :: ${event.toString} ")
       }
     
   }
   
   /**
    *  If they implement our Logging trait, make sure we get their output into to correct 
    *    Console object
    */
   def modifyLogger( obj : Any ) = {
     obj match {
       case logging : Logging => {
         val scalaOut = Console.out
         if( logging.log.isInstanceOf[LogbackLogger]) {
         val logbackLogger : LogbackLogger = logging.log.asInstanceOf[LogbackLogger]  
         logbackLogger.addAppender( new  ScalaConsoleAppender )
         Console.println(s" Adding ${obj} with LogBack logging ")
        } else {
         System.out.println(s" Trying to Adding ${obj} with LogBack logging " )
         Console.println(s" Trying to  Adding ${obj} with LogBack logging  "  )
          
        }
       }
       case _ => {
         ///Console.println(s"${obj} Not a Logging interface ") 
         ///System.out.println(s" ${obj} Not a Logging interface ") 
       }
     }
   }
    
}