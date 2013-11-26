package com.klout.satisfaction
package executor
package track

import org.joda.time.LocalTime
import hive.ms.SLA
import org.joda.time.Period
import actors.ProofEngine
import hive.ms.Hdfs
import us.theatr.akka.quartz._
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.Path

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
case class TrackFactory(val trackPathURI : java.net.URI, 
    val baseTrackPath : String = "/user/satisfaction",
    val scheduler : TrackScheduler = TrackScheduler) {
   private implicit val hdfs = new Hdfs(trackPathURI.toASCIIString)
   
   
   private var trackMap : collection.mutable.Map[TrackDescriptor,Track] = collection.mutable.HashMap[TrackDescriptor,Track]()
   
   
   

   /** 
    *  Get all the tracks which have been deployed to HDFS under the
    *   Satisfaction base track path.
    */
   def getAllTracks : Seq[TrackDescriptor] = {
      val trackRoot = new Path(  trackPathURI.toString + "/"  + this.baseTrackPath )
      val allPaths = hdfs.listFilesRecursively(trackRoot)
      allPaths.filter( _.getPath.getName.startsWith("version_")).map( fs => {
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
   def registerTrack( trackDesc : TrackDescriptor ) = {
     
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
      if( !hdfs.exists(trackPath)) {
        throw new RuntimeException( s"No Track found under ${trackPathURI.toString}/${baseTrackPath} for descripter ${trackDesc} ")
      }
      val propPath = new Path(trackPath.toUri   + "/satisfaction.properties")
      if( !hdfs.exists(propPath)) {
        throw new RuntimeException( s"No properties file found for Track found under ${trackPath.toUri.toString} for descripter ${trackDesc} ")
      }
      
      val inStream = hdfs.open( propPath)
      val trackProps = Substituter.readProperties( inStream)
      
      val trackClassName = trackProps.get( "satisfaction.track.class").get
      val trackJar = trackProps.get( "satisfaction.track.jar").get
     
      val jarPath = new Path(trackPath + "/" + trackJar)
      println(s" Getting track from jar ${jarPath.toUri} ")
      val trackClazzOpt = loadTrackClass( new Path(trackPath  + "/" + trackJar) , trackClassName )
      trackClazzOpt match {
        case Some(trackClazz)  =>
         val track = trackClazz.newInstance 
      //// XXX mutability of track properties
         track.trackProperties =  Substitution(trackProps) 
         trackProps.get("satisfaction.track.schedule") match {
           case Some(schedStr) =>
              println( "Scheduling "+ trackDesc.trackName + " at " + schedStr)
              val sched = TrackSchedule(schedStr)
              scheduler.scheduleTrack(trackDesc,sched)
           case None =>
              println(" No schedule defined for track " + trackDesc.trackName )
         }
         
     
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
     if( hdfs.isDirectory( jarPath) ) {
       
       hdfs.listFiles( jarPath).filter( _.getPath.getName.endsWith(".jar")).map( _.getPath.toUri.toURL).toArray
     } else {
        val hdfsUrl = jarPath.toUri.toURL
        println(" HDFS URL is " + hdfsUrl.toString)
        Array( hdfsUrl)
       
     }
   }
   
   def loadTrackClass( jarPath : Path , trackClassName : String ) : Option[Class[_ <: Track]]  = {
     try {
      
      val urlClassloader = new java.net.URLClassLoader(jarURLS( jarPath), this.getClass.getClassLoader)
      Thread.currentThread.setContextClassLoader(urlClassloader)
      
      //// Accessing object instances 
      ///val scalaName = if (trackClassName endsWith "$") trackClassName else (trackClassName + "$")
      val scalaName = trackClassName
      val scalaClass = urlClassloader.loadClass( scalaName)
      println( " Scala Class is " + scalaClass.getCanonicalName())
      
      println(" Scala Parent class = " + scalaClass.getSuperclass() + " :: " + scalaClass.getSuperclass.getCanonicalName())
      
      val t1Class = classOf[Track]
      println(" Track class = " + t1Class.getSuperclass() + " :: " + t1Class.getSuperclass.getCanonicalName())
      
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
	  val sb : StringBuilder = new StringBuilder
	  sb ++= this.trackPathURI.toString
	  if(! sb.toString.endsWith( "/")) {
	    sb += '/'
	  }
	  sb ++= this.baseTrackPath
	  sb ++= "/track/"
	  sb ++= td.trackName
	  if(! td.forUser.equals( td.trackName)) {
	    sb ++= "/user/"
	    sb ++= td.forUser
	  }
	  td.variant match {
	    case Some(vr) =>
	      sb ++= "/variant/"
	      sb ++=  vr
	    case None =>
	  }
	  sb ++= s"/version_${td.version}/"
	  
	  new Path(sb.toString)
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

object TrackFactory extends TrackFactory( new java.net.URI("hdfs://jobs-dev-hnn:8020"), "/user/satisfaction", TrackScheduler)