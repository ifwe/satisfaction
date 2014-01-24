package com.klout
package satisfaction
package track

import collection.JavaConversions._

/// XXX abstraction of filesystem ...
import fs._

/// XXX FIXME dependency inject the Filesystem

/**
 *  Class for Executor to access deployed Tracks (i.e Projects)
 *  
 *  Tracks are deployed in a well-defined directory on HDFS,
 *  where the Track Name is defined as a path relative to a 
 *   a base path. It is assumed the project has been deployed with 
 *   the sbtUpload plugin or equivalent.
 *  
 *   Different satisfiers will expect different files in different locations
 *   ( Hive vs Scoozie ), but in the Track path, there will be a property 
 *   file containing all the ancillary properties for the Track, and 
 *   various properties to be accessed by the satisfiers when a Track is registered.
 *   
 *  
 */
case class TrackFactory(val trackFS : FileSystem, 
    val baseTrackPath : String = "/user/jerome/satisfaction",
    val scheduler : TrackScheduler = TrackScheduler) {
   private implicit val localFS : FileSystem =  LocalFileSystem(System.getProperty("user.dir"))
   
   ///  XXX Use Local FS abstraction
   val auxJarsPathBase = new java.io.File(System.getProperty("user.dir") + "/auxJars")
   
   
   private var trackMap : collection.mutable.Map[TrackDescriptor,Track] = collection.mutable.HashMap[TrackDescriptor,Track]()
   
   
   

   /** 
    *  Get all the tracks which have been deployed to HDFS under the
    *   Satisfaction base track path.
    */
   def getAllTracks : Seq[TrackDescriptor] = {
       /// XXX Have filesystem return path as well as uri 
      val trackRoot = new Path(  trackFS.uri.toString)  / this.baseTrackPath / "track" 
      System.out.println( " TrackRoot is " + trackRoot)
      val allPaths = trackFS.listFilesRecursively(trackRoot)
      allPaths.filter(_.isDirectory).foreach( fs => { println(" TRACK PATH IS " + fs.getPath ) } )
      allPaths.filter(_.isDirectory).filter( _.getPath.name.startsWith("version_")).map( fs => {
          parseTrackPath( fs.getPath )       
      }) 
   }
   
   
  
   
   /**
    *  Parse the path of a deployed Track on HDFS
    *    to the implied track descriptor 
    */
   def parseTrackPath( path : Path) : TrackDescriptor = {
     val pathStr  = path.toUri.toString 
     println(" ParseTrackPath " + pathStr)
     val tailStr = pathStr.substring( pathStr.indexOf( this.baseTrackPath) + baseTrackPath.length + 1) 
     
     val parts = tailStr.split( "/") 
     /// Assert part[0] = "track"
     var idx = 1;
     var user : String = null
     var variant : String = null
     var version : String = null
     val trackName = parts(idx)
     idx = idx + 1
     if( parts(idx).equals( "user" )) {
       idx = idx +1 
       user = parts(idx)
       idx = idx +1
     } else {
       user = trackName
     }
     if( parts(idx) .equals("variant")) {
       idx = idx + 1
       variant = parts(idx)
       idx = idx + 1
     }
     println(" PARTS IDX = " + parts(idx))
     version = parts(idx).substring("version_".length)
     
     new TrackDescriptor( trackName, user, version,  if (variant != null)  Some(variant) else None) 
   }
  
   /** 
    *  Perform some initialization work,
    *   (needed by the various satisfiers of goals).
    *  when a project is first deployed  
    */
   def registerTrackXXXX( trackDesc : TrackDescriptor ) = {
     ////
     
   }
   
   /**
    * Once a track has been loaded, 
    *   make sure that it has setup everything it needs to run ..
    */
   def initializeTrack( track: Track , trackProps : Map[String,String] ) = {
     println(s" Initializing Track ${track.descriptor.trackName}")
     track.setTrackProperties(Substitution(trackProps ))
                
     val trackPath = getTrackPath( track.descriptor )
     
     trackProps.get( "satisfaction.track.auxjar") match {
     case Some(trackAuxJar) =>
        val auxJarsPath = new java.io.File( this.auxJarsPathBase.getPath() + "/" + track.descriptor.trackName)
        auxJarsPath.mkdirs
        val auxJarPath = new Path(trackPath + "/" + trackAuxJar)
        trackFS.listFiles( auxJarPath ).foreach { fs : FileStatus => {
    	    println(s" Copying  ${fs.getPath} to local aux jars path ${auxJarsPath.getPath}" )
    	   trackFS.copyToFileSystem( localFS, fs.getPath, new Path( "file://" +  auxJarsPath.getPath ) )
         }}
        
        track.setAuxJarFolder( auxJarsPath)
        track.registerJars( auxJarsPath.getPath)
     case None =>
     }
       
     trackProps.get("satisfaction.track.schedule") match {
      case Some(schedStr) =>
           println( "Scheduling "+ track.descriptor.trackName + " at " + schedStr)
          val sched = TrackSchedule(schedStr)
          scheduler.scheduleTrack(track.descriptor ,sched)
      case None =>
           println(" No schedule defined for track " + track.descriptor.trackName )
     }
     
     track.setTrackPath( getTrackPath( track.descriptor))
     
     //// Now that has been initialized, go through all the Track's Goals, 
     //// and  inject the Track value ...
     track.allGoals.foreach { goal  : Goal => {
    	 if( goal.isInstanceOf[TrackOriented]) {
    		 val trackGoal : TrackOriented = goal.asInstanceOf[TrackOriented]
    		 trackGoal.setTrack(track)
    		 /// Inject the DataOutputs ???
    		 goal.evidence.foreach { outp => {
    			 if( outp.isInstanceOf[TrackOriented]) {
    			   val trackOutp = outp.asInstanceOf[TrackOriented]
    			   trackOutp.setTrack(track)
    			 }
    		 }}
    	 }
      }
     }
     
   }
   
   
   /**
    *  Given a track,
    */
   def getDescriptor( track: Track ) : TrackDescriptor = {
      track.descriptor
   }
   
   
   /**
    *  Get the actual  Track object, with the top level goals
    *     to be satisfied
    */
   def generateTrack( trackDesc : TrackDescriptor ) : Option[Track] = {
     try {
      val trackPath = getTrackPath( trackDesc)
      if( !trackFS.exists(trackPath)) {
        throw new RuntimeException( s"No Track found under ${trackFS.uri}/${baseTrackPath} for descripter ${trackDesc} ")
      }
      val propPath = new Path(trackPath.toUri   + "/satisfaction.properties")
      if( !trackFS.exists(propPath)) {
        throw new RuntimeException( s"No properties file found for Track found under ${trackPath.toUri.toString} for descripter ${trackDesc} ")
      }
      
      val inStream = trackFS.open( propPath)
      val trackProps = Substituter.readProperties( inStream)
      
      val trackClassName = trackProps.get( "satisfaction.track.class").get
      val trackJar = trackProps.get( "satisfaction.track.jar").get
     
      val jarPath = new Path(trackPath + "/" + trackJar)
      println(s" Getting track from jar ${jarPath.toUri} ")
      val trackClazzOpt = loadTrackClass( new Path(trackPath  + "/" + trackJar) , trackClassName )
      trackClazzOpt match {
        case Some(trackClazz)  =>
         val track = trackClazz.newInstance 
         track.descriptor = trackDesc
         initializeTrack( track,  trackProps)
      	 Some(track)
        case None => None
      }
      
     } catch {
       case exc : Throwable =>
         /// Unexpected 
         exc.printStackTrace(System.err)
         None
     }
   }
   
   def jarURLS( jarPath : Path ) : Array[java.net.URL] = {
     if( trackFS.isDirectory( jarPath) ) {
       
       trackFS.listFiles( jarPath).filter( _.getPath.toString.endsWith(".jar")).map( _.getPath.toUri.toURL).toArray
     } else {
        val hdfsUri = jarPath.toUri
        if( jarPath.toString.endsWith(".jar")) {
          ///val jarUrl = new java.net.URL( s"jar:/${hdfsUri}")
          val jarUrl = hdfsUri.toURL
          Array( jarUrl)
        } else {
          val hdfsUrl = hdfsUri.toURL
          Array( hdfsUrl)
        }
     }
   }
   
   def loadTrackClass( jarPath : Path , trackClassName : String ) : Option[Class[_ <: Track]]  = {
     try {
      
      val urlClassloader = new java.net.URLClassLoader(jarURLS( jarPath), this.getClass.getClassLoader)
      Thread.currentThread.setContextClassLoader(urlClassloader)
      
      urlClassloader.getURLs.foreach( earl => {
         System.out.println(s" using jar URL $earl ")
      } ) 
      
      //// Accessing object instances 
      ///val scalaName = if (trackClassName endsWith "$") trackClassName else (trackClassName + "$")
      //// XXX allow scala companion objects 
      val scalaName = trackClassName
      
      val scalaClass = urlClassloader.loadClass( scalaName)
      println( " Scala Class is " + scalaClass.getCanonicalName())
      
      println(" Scala Parent class = " + scalaClass.getSuperclass() + " :: " + scalaClass.getSuperclass.getCanonicalName())
      
      val t1Class = classOf[Track]
      println(" Track class = " + t1Class + " :: " + t1Class.getCanonicalName())
      
      val tClass = scalaClass.asSubclass( classOf[Track])
      
      println( " Track Scala Class is " + tClass + " :: " + tClass.getCanonicalName())
      Some(tClass)
     } catch {
         case e : Throwable =>
           /// XXX Case match any catch all throwable
           e.printStackTrace(System.out)
           println(" Could not find Track class ")
           None
     }
     
     
   }
   
   /**
    *  From a track descriptor, generate the path corresponding to the deployment
    */
   def getTrackPath( td : TrackDescriptor ) : Path = {
     /// XXX get Path from fs
      var tp = new Path( trackFS.uri.toString)
      tp = tp / baseTrackPath
      tp = tp /  "track" / td.trackName
	  if(! td.forUser.equals( td.trackName)) {
         tp = tp / "user" / td.forUser 
	  }
	  td.variant match {
	    case Some(vr) =>
	      tp = tp / "variant" / vr
	    case None =>
	  }
	  tp = tp / s"version_${td.version}"
      
      tp
   }
   
   
   def versionSort( lft : TrackDescriptor , rt : TrackDescriptor ) : Boolean = {
     //// XXX  parse to get integral value 
     /// of the version string
       lft.version.compareTo( rt.version) > 0
   }
   
   def getTrack( trackDesc : TrackDescriptor ) : Option[Track] = {
     if( !trackDesc.version.equals("LATEST")) {
    	 if( trackMap.contains(trackDesc)) {
    		 val track = trackMap.get( trackDesc ).get
    				 Some(track)
    	 } else {
    		 val trackOpt = generateTrack( trackDesc)
    		 trackOpt match {
    		   case Some(track) =>
    		     trackMap.put( trackDesc, track)
    		     Some(track)   
    		   case None => None
    		 }
    	 }
     } else {
       val latestTd = getAllTracks.filter( _.trackName.equals(trackDesc.trackName)  ).
           toList.sortWith( versionSort )(0)
            
       getTrack( latestTd)
     }
   }
}

/// XXX Fix me 
object TrackFactory extends TrackFactory( null, "/user/satisfaction", TrackScheduler) {
  
}