package com.klout
package satisfaction

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.io.FileInputStream
import java.io.File
import java.lang.reflect.Method
import java.net.URLClassLoader
import satisfaction.fs.Path
import scala.io.Source
import satisfaction.fs.FileSystem
import satisfaction.fs.LocalFileSystem

/**
   *  Case class describing how a track can be deployed.
   *  There might be multiple versions of a track, by different users,
   *   for different possible variants.
   *   
   *   For example, in a staging or dev environment, multiple users could be
   *    working on the same track, possibly with multiple features or variants.
   *   In a production system, one could imagine multiple tracks, running in parallel,
   *    in order to compare results before releasing.
   */
  case class TrackDescriptor( val trackName : String, val forUser : String, val version : String, variant : Option[String] = None) {
   
     override def toString() = {
       s"TrackDescriptor::name=$trackName forUser=$forUser Version=$version Variant=$variant"
     }
 }
  
  object TrackDescriptor  {  
     def apply( tName : String ) : TrackDescriptor = {
        new TrackDescriptor( tName, tName, "LATEST", None) 
     }
  }
  
/**
  * A Track defines a cohesive DataFlow,
  *    a set of related 
  *   Goals and their corresponding 
  *     dependencies
  */
case class Track( 
    var descriptor : TrackDescriptor )  {

  
    /// XXX Track initialization
    def init : Unit = { }
  
  
    val topLevelGoals : collection.mutable.Set[Goal] = collection.mutable.Set.empty
    
    def addTopLevelGoal( goal : Goal ) : Track = {
      topLevelGoals.add( goal)
      this
    }
    
    
    //// XXX Split out tracks and TrackContext ...
    /// Define filesystems which Tracks can read and write to
    /// Define as local, to avoid unnecessary dependencies
    //// XXX FIXME -- Allow implicit HDFS to be in scope on object creation
    implicit var hdfs : FileSystem = LocalFileSystem
    implicit val track : Track = this
    
    
    /**
     *  Ay yah ... XXX FIXME
     */
    lazy val allGoals: Set[Goal] = {
        def allGoals0(toCheck: List[Goal], accum: Set[Goal]): Set[Goal] = {
            toCheck match {
                case Nil => accum
                case current :: remaining =>
                    val currentDeps =
                        if (accum contains current) Nil
                        else current.dependencies map (_._2)

                    allGoals0(remaining ++ currentDeps, accum + current)
            }
        }

        allGoals0(topLevelGoals.toList, Set.empty)
    }

    val externalGoals :Set[Goal] = Set.empty
    
    def getWitnessVariables : Set[Variable[_]] = {
      topLevelGoals.flatMap( _.variables ).toSeq.distinct.toSet
    }
    
    /**
     *  Attach a set 
     *   of properties along with the Track
     * 
     */
     private var _trackProperties : Witness = null
     def trackProperties : Witness = {
       _trackProperties
     }

     private var _trackPath : Path = null;
     
     def trackPath : Path = {
       _trackPath
     }
     def setTrackPath( path : Path ) = {
       _trackPath = path
     }
     
     def setTrackProperties( props : Witness) = {
    	  _trackProperties = props
     }
     
   /** 
    *  Read a resource from HDFS 
    *  XXX Move to Engine ???
    * 
    */
   def getResource(   resourceFile : String ) : String  = {
      new String(hdfs.readFile( resourcePath / resourceFile ))
   }
   
   def hasResource( resourceFile : String ) : Boolean  = {
      hdfs.exists( resourcePath / resourceFile ) 
   }
   
   def listResources : Seq[String]  = {
      hdfs.listFiles(  resourcePath ).map( _.path.name )
   }
   
   def resourcePath : Path = {
     if( _trackProperties != null) {
       val resourceDir = _trackProperties.raw.get("satisfaction.track.resource") match {
         case Some(path) => path
         case None => "resources" 
       } 
       _trackPath /  resourceDir
     } else {
       _trackPath /  "resources"
     }
   }
   
   def libPath : Path = {
     if( _trackProperties != null) {
       val libDir = _trackProperties.raw.get("satisfaction.track.lib") match {
         case Some(path) => path
         case None => "lib" 
       } 
       _trackPath /  libDir
     } else {
       _trackPath /  "lib"
     }
   }

   
    def getTrackProperties(witness: Witness): Witness = {
      
         val YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd")
    
        var projProperties : Witness =  trackProperties

        ///// Some munging logic to translate between camel case 
        //// and  underscores
        ////   and to do some simple date logic

        if (witness.contains(Variable("dt"))) {
            //// convert to Date typed variables... 
            //// not just strings 
            var jodaDate = YYYYMMDD.parseDateTime(witness.get(Variable("dt")).get)
            ////val assign : VariableAssignment[String] = ("dateString" -> YYYYMMDD.print(jodaDate))
            val dateVars = Witness((Variable("dateString") -> YYYYMMDD.print(jodaDate)),
                (Variable("yesterdayString") -> YYYYMMDD.print(jodaDate.minusDays(1))),
                (Variable("prevdayString") -> YYYYMMDD.print(jodaDate.minusDays(2))),
                (Variable("weekAgoString") -> YYYYMMDD.print(jodaDate.minusDays(7))),
                (Variable("monthAgoString") -> YYYYMMDD.print(jodaDate.minusDays(30))));

            println(s" Adding Date variables ${dateVars.raw.mkString}")
            projProperties = projProperties ++ dateVars
            projProperties = projProperties.update(VariableAssignment("dateString", witness.get(Variable("dt")).get))
        }

        projProperties ++ witness

    }


}

object Track {
  
   def trackForGoal(  goal : Goal) : Track = {
     new Track( TrackDescriptor( goal.name) ).addTopLevelGoal(goal)
   }
   
    def apply( trackName : String) : Track = {
       new Track( TrackDescriptor(trackName) )
    }
    
    def localTrack( td : String,  path : Path ) : Track = {
       trackForPath( td, LocalFileSystem, path) 
    }
    
    def trackForPath( tname : String, fs : FileSystem, path : Path ) : Track = {
      val tr = new Track(TrackDescriptor( tname) )
      tr.hdfs = fs
      tr.setTrackPath( path)
      val props = Substituter.readProperties( fs.open( path / "satisfaction.properties") )
      tr.setTrackProperties( Witness(props))
       
      tr
    }
    
    def apply( trackName : String, topLevelGoals : Set[Goal]) : Track = {
       val tr = new Track( TrackDescriptor(trackName)) 
       topLevelGoals.foreach( tr.topLevelGoals.add( _ ))
       tr
    }
    
    def apply( trackName : String, topLevelGoal : Goal) : Track = {
       new Track( TrackDescriptor(trackName) ).addTopLevelGoal(topLevelGoal) 
    }
}

