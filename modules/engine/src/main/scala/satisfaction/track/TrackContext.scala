package satisfaction
package track

import satisfaction.fs.FileSystem
import org.joda.time.format.DateTimeFormat
import satisfaction.fs.Path
import java.io.File
import java.net.URLClassLoader
import java.lang.reflect.Method



/**
 *  TrackContext is a Track, along
 *    with addiitonal context,
 *    
 *    ie, the Associated filesystem, and 
 *      additional properties
 *
 */
case class TrackContext( val track : Track,
					     val trackPath : Path,
                         val trackProperties : java.util.Properties,
                         val hdfs : FileSystem) {
  
  
    def getResource(   resourceFile : String ) : String  = {
      println(" GET HDFS is " + hdfs)
      hdfs.readFile( resourcePath / resourceFile ).mkString
   }
   
   def listResources : Seq[String]  = {
      println(" LIST  HDFS is " + hdfs)
      hdfs.listFiles(  resourcePath ).map( _.path.name )
   }
   
   def resourcePath : Path = {
     val resourceDir: String = trackProperties.get("satisfaction.track.resource", "resource").toString
      trackPath /  resourceDir
   }
   
     
   /// XXX File to LocalFileSystm ???
    ///private var _auxJarFolder : Path= null
    private var _auxJarFolder : File = null
     
     
     def auxJarFolder : File = {
    	if( _auxJarFolder != null) { 
    	   _auxJarFolder 
    	}
  	    throw new RuntimeException("AuxJars accessed, but not registered yet !!!")	
     }

     def setAuxJarFolder( auxJar : File) = {
       _auxJarFolder = auxJar
     }
        /// Want to make it on a per-project basis
    /// but for now, but them in the auxlib directory
     
     
    def registerJars( folder : String): Unit = {
        _auxJarFolder = new File( folder)
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