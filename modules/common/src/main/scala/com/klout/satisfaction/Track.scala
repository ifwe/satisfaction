package com.klout.satisfaction

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.io.FileInputStream
import java.io.File
import java.lang.reflect.Method
import java.net.URLClassLoader

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
       s"TrackDescriptor::name=$trackName forUser=$forUser Version= $version Variant=$variant"
     }
 }
  
  object TrackDescriptor  {  
     def apply( tName : String ) : TrackDescriptor = {
        new TrackDescriptor( tName, tName, "LATEST", None) 
     }
  }
  
  /**
   * A Track 
   */

case class Track(
    descriptor : TrackDescriptor,
    topLevelGoals: Set[Goal] ) {

    def apply( trackName : String, topLevelGoals : Set[Goal]) : Track = {
       Track( TrackDescriptor(trackName), topLevelGoals) 
    }
    def apply( trackName : String, topLevelGoal : Goal) : Track = {
       Track( TrackDescriptor(trackName), Set(topLevelGoal)) 
    }
    
    
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

    lazy val internalGoals: Set[Goal] = allGoals filter (_.satisfier.isDefined)

    lazy val externalGoals: Set[Goal] = allGoals filter (_.satisfier.isEmpty)
    
    
    def getWitnessVariables : Set[Variable[_]] = {
      topLevelGoals.flatMap( _.variables ).toSeq.distinct.toSet
    }
    
    /**
     *  Attach a set 
     *   of properties along with the Track
     * 
     */
     var trackProperties : Substitution = null

     
     def readProperties( pathString : String ) = {
       trackProperties = Substitution( Substituter.readProperties(new FileInputStream( pathString )))
     }
     
     var auxJarFolder : File = null
        /// Want to make it on a per-project basis
    /// but for now, but them in the auxlib directory
     
     
    def registerJars( folder : String): Unit = {
        auxJarFolder = new File( folder)
        this.auxJarFolder.listFiles.filter(_.getName.endsWith("jar")).foreach(
            f => {
                println(s" Register jar ${f.getAbsolutePath} ")
                val jarUrl = "file://" + f.getAbsolutePath
                
                /// Need to add to current loader as well, not just thread loader,
                //// because some classes call Class.forName
                ////  Hive CLI would have jar in enclosing classpath.
                //// To avoid spawning new JVM, call
                val currentLoader = this.getClass.getClassLoader
                if( currentLoader.isInstanceOf[URLClassLoader]) {
                  val currentURLLoader = currentLoader.asInstanceOf[URLClassLoader]
                  val method : Method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[java.net.URL])
                  method.setAccessible(true)
                  println(s" Adding to current classpath $jarUrl")
                  method.invoke( currentURLLoader, new java.net.URL(jarUrl))
                } else {
                   println(s" Current classloader is of type ${currentLoader.getClass} ; Cannot append classpath !!!") 
                }
                println(s" Adding to current classpath $jarUrl")
            }
        )
        

    }


}


trait TemporalVariable {
  
    def getObjectForTime( dt : DateTime) : Any
}


trait TrackOriented {

    val YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd")
    
    var track : Track = null

    def trackName =  { track.descriptor.trackName }
      
      
      
    def setTrack( track : Track ) = {
    	this.track = track
    }

    def getTrackProperties(witness: Substitution): Substitution = {
        var projProperties : Substitution =  track.trackProperties

        ///// Some munging logic to translate between camel case 
        //// and  underscores
        ////   and to do some simple date logic

        if (witness.contains(Variable("dt"))) {
            //// convert to Date typed variables... 
            //// not just strings 
            var jodaDate = YYYYMMDD.parseDateTime(witness.get(Variable("dt")).get)
            ////val assign : VariableAssignment[String] = ("dateString" -> YYYYMMDD.print(jodaDate))
            val dateVars = Substitution((Variable("dateString") -> YYYYMMDD.print(jodaDate)),
                (Variable("yesterdayString") -> YYYYMMDD.print(jodaDate.minusDays(1))),
                (Variable("prevdayString") -> YYYYMMDD.print(jodaDate.minusDays(2))),
                (Variable("weekAgoString") -> YYYYMMDD.print(jodaDate.minusDays(7))),
                (Variable("monthAgoString") -> YYYYMMDD.print(jodaDate.minusDays(30))));

            println(s" Adding Date variables ${dateVars.raw.mkString}")
            projProperties = projProperties ++ dateVars
            projProperties = projProperties.update(VariableAssignment("dateString", witness.get(Variable("dt")).get))
        }

        /// XXX Other domains won't have social networks ...
        if (witness.contains(Variable("network_abbr"))) {
            projProperties = projProperties + (Variable("networkAbbr") -> witness.get(Variable("network_abbr")).get)
            //// needs to be handled outside of satisfier ???
            /// XXX need way to munge track properties
            projProperties = projProperties + (Variable("featureGroup") -> "3")
            ///projProperties = projProperties.update(VariableAssignment("networkAbbr", witness.get(Variable("network_abbr"))))
        }

        projProperties ++ witness

    }
}
