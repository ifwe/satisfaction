package satisfaction
package track

import collection.JavaConversions._
import fs._
import java.util.Properties
import satisfaction.Recurring
import satisfaction.Track.MajorMinorPatch
import satisfaction.engine.actors.JobRunner


/**
 *  Class for Executor to access deployed Tracks (i.e Projects)
  class TracksUnavailableException( exc : Throwable ) extends RuntimeException
  class TracksUnavailableException( exc : Throwable ) extends RuntimeException
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
    val baseTrackPath : Path = Path("/user/satisfaction"),
    val schedulerOpt : Option[TrackScheduler] = None,
    val defaultConfig : Option[Witness] =  None) extends Logging {
  
  implicit val hdfs = trackFS
  val localFS = LocalFileSystem
  
  
  val initScheduler : Unit = {
     schedulerOpt match {
       case Some(sched) => sched.trackFactory = this
       case None => {
          warn(" No Scheduler available") 
       }
     } 
 }
  
 /**
  *  Monitor the memory usage, and die 
  *  when we have exhausted all of the PermGen space
  */
 val initPoisonPill = {
   PoisonPill()
 } 
   
 private val _trackMap : collection.mutable.Map[TrackDescriptor,Track] = collection.mutable.HashMap[TrackDescriptor,Track]()
 def trackMap : collection.immutable.Map[TrackDescriptor,Track] =  _trackMap.toMap
  
   def initializeAllTracks =  {
      getLatestTracks.foreach( track => {
        val tr = generateTrack( track)
         //_trackMap.put( track, tr)
      })
  }
  
 
 /**
  *  XXX Move caching to utility ...
  *  XXX allow it to be resettable 
  */
 class  Cacheable[T](val  maxAge : Long, genFunc : ()  => T ) {
   private var _lastAccessed : Long =  System.currentTimeMillis()
   private var _cached : Option[T]  = None
  
   def get() : T = {
     _cached match {
       case None => {
         _cached = Some(genFunc() )
         _cached.get
       }
       case Some(tObj) => {
         if( System.currentTimeMillis - _lastAccessed >= maxAge ) {
           _cached = Some( genFunc()  )
           _cached.get
         } else {
           tObj 
         }
       }
     }
   } 
}
   
   /** 
    *  Get all the tracks which have been deployed to HDFS under the
    *   Satisfaction base track path.
    */
   def getAllTracks : Seq[TrackDescriptor] = {
      _cachedAllTracks.get
   }
   
   private val _cachedAllTracks = new Cacheable( 60*1000*30, _getAllTracks )
   
   private def _getAllTracks() : Seq[TrackDescriptor] = {
     try {
      val trackRoot : Path =  baseTrackPath / "track" 
      
      val allPaths = trackFS.listFilesRecursively(trackRoot)
      allPaths.filter(_.isDirectory).filter( _.path.name.startsWith("version_")).map( fs => {
          parseTrackPath( fs.path )       
      }) 
     } catch {
       case exc: Exception => {
         error("Error while getting Tracks; Maybe HDFS is down? ", exc)
         
         notifyTrackUnavailable( exc)
         throw new TrackFactory.TracksUnavailableException( exc);
       }
     }
   }
  
   def getLatestTracks : Seq[TrackDescriptor] = {
      val trackDescMap: Map[TrackDescriptor,MajorMinorPatch] = getAllTracks.foldLeft(  Map[TrackDescriptor,MajorMinorPatch]() )( (map,td) => {
        val lookup = td.latest
        if( map.contains( lookup)) {
          val latestTD = map.get( td.latest).get
          if( MajorMinorPatch( td.version).compareTo( latestTD) >0  ) {
             map updated( lookup, MajorMinorPatch(td.version))
          } else {
            map
          }
        } else {
          map + ( lookup -> MajorMinorPatch(td.version) )
        }
      })
      
      trackDescMap.map( { case (td,mmp) => {
           td.withVersion( mmp.toString )
       }  }).toSeq
   }
   
   def getLatestTrack( trackDesc : TrackDescriptor ) = {
       val latestTD = getAllTracks.filter( _.equalsWoVersion(trackDesc)  ).
           toList.sortWith( versionSort )(0)
       info( s"Getting track ${trackDesc.trackName} with latest version ${latestTD.version} ")     
       getTrack( latestTD)
   }
  
   /**
    *  Do something if HDFS is down 
    */
   def notifyTrackUnavailable( exc : Throwable) {
     
   }
   
   /**
    *  Parse the path of a deployed Track on HDFS
    *    to the implied track descriptor 
    */
   def parseTrackPath( path : Path) : TrackDescriptor = {
     val pathStr  = path.toUri.toString 
     val tailStr = pathStr.substring( pathStr.indexOf( this.baseTrackPath.pathString) + baseTrackPath.pathString.length + 1) 
     
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
     version = parts(idx).substring("version_".length)
     
     new TrackDescriptor( trackName, user, version,  if (variant != null)  Some(variant) else None) 
   }
  
   /**
    * Once a track has been loaded, 
    *   make sure that it has setup everything it needs to run ..
    */
   def initializeTrack( track: Track , trackMap : Map[String,String] ) = {
     println(s" Initializing Track ${track.descriptor.trackName}")
     info(s" Initializing Track ${track.descriptor.trackName}")

     val trackProps : Witness =  {
         defaultConfig match {
           case Some(hadoopConfig)  => hadoopConfig ++ Witness(trackMap)
           case None => Witness(trackMap)
        }
     }
     track.setTrackProperties(trackProps)
                
     val trackPath = getTrackPath( track.descriptor )

     track.setTrackPath( getTrackPath( track.descriptor))
     /// XXX JDB FIXME
     /// XXX Allow implicit to be set on object creation ...
     track.hdfs = hdfs 
     
     /**
      *  Set the ThreadContextLoader in init
      */
     JobRunner.threadPreserve(track) {
        track.init
     }
     
     schedulerOpt match {
       case Some(scheduler) =>
          info(" Scheduling Track ")
          //// Check that scheduling flag isn't set to false          
          val checkDoSched = trackMap.getOrElse( "satisfaction.track.scheduleFlag", "true").toBoolean
          if( checkDoSched) {
            scheduler.scheduleTrack( track, false)
          } else {
            warn(s" satisfaction.track.scheduleFlag set to false; not scheduling ${track.descriptor.trackName} ")
          }
        case None =>
          warn(" No scheduler instantiated; not scheduling") 
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
   private def generateTrack( trackDesc : TrackDescriptor ) : Option[Track] = {
     try {
      info(s" Generating Track ${trackDesc.trackName} :: ${trackDesc.version} ")
      val trackPath = getTrackPath( trackDesc)
      if( !trackFS.exists(trackPath)) {
        throw new RuntimeException( s"No Track found under ${trackFS.uri}/${baseTrackPath} for descripter ${trackDesc} ")
      }
      val propPath = new Path(trackPath.toUri.toString)   / "satisfaction.properties"
      if( !trackFS.exists(propPath)) {
         error( s"No properties file found for Track found under ${trackPath.toUri.toString} for descripter ${trackDesc} ")
         return None
      }
      info( s" Loading track properties at $propPath ")
      
      val inStream = trackFS.open( propPath)
      val trackProps = Substituter.readProperties( inStream)

      val trackClassName = trackProps.get( "satisfaction.track.class") match {
        case Some(name) => name
        case None =>
          error("Unable to find track class in properties file ; TrackProps is  " + trackProps)
          return None
      }
      val trackJar = trackProps.get( "satisfaction.track.jar").getOrElse("lib")
     
      val jarPath = trackPath / trackJar
      info(s" Getting track from jar ${jarPath.toUri} ")
      val trackClazzOpt = loadTrackClass( trackProps, trackPath  / trackJar, trackClassName )
      trackClazzOpt match {
        case Some(trackClazz)  =>
         val track = trackClazz.newInstance 
          
          //// XXX   Right way to do scala instantiation...
          //// FIXME ???
         ////val track : Track =  trackClazz.getField("MODULE$").get(null).asInstanceOf[Track]
          
         track.setDescriptor( trackDesc )
         this._trackMap.put( trackDesc, track)
         initializeTrack( track,  trackProps)
      	 Some(track)
        case None => 
          warn("initializeTrack returned None")
          None
      }
      
     } catch {
       case exc : Throwable =>
         /// Unexpected 
         exc.printStackTrace(System.err)
         error(s"Unable to instantiate Track Class s${exc.getMessage}", exc)
         None
     }
   }
   
  def loadTrackClass( trackProps : Map[String,String], jarPath : Path , trackClassName : String ) : Option[Class[_ <: Track]]  = {
       val trackLoader : TrackLoader = if( trackProps.contains("satisfaction.track.trackLoaderClass") ) {
          val loaderClassName = trackProps.get("satisfaction.track.trackLoaderClass").get
          try {
             val trackLoaderClass = Class.forName( loaderClassName).asInstanceOf[Class[TrackLoader]]
          
             val constructor = trackLoaderClass.getConstructor( classOf[TrackFactory],  classOf[Map[String,String]] )
             constructor.newInstance(this, trackProps)
          } catch {
            case unexpected : Throwable => {
              error(s"Unable to create TrackLoader $loaderClassName because of ${unexpected.getMessage} ; Defaulting to DefaultTrackLoader ", unexpected)
              new DefaultTrackLoader(this,trackProps) 
            }
          }
          
       } else {
          new DefaultTrackLoader(this,trackProps) 
       }
      
      
       val classLoader = trackLoader.createClassLoader(jarPath)
       
       trackLoader.loadTrackClass(trackClassName, classLoader)
      
   }
   
   /**
    *  From a track descriptor, generate the path corresponding to the deployment
    */
   def getTrackPath( td : TrackDescriptor ) : Path = {
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
       MajorMinorPatch( lft.version )  > MajorMinorPatch( rt.version)
   }
   
   def getTrack( trackDesc : TrackDescriptor ) : Option[Track] = {
     if( !trackDesc.version.equals("LATEST")) {
    	 if( trackMap.contains(trackDesc)) {
    		 val track = trackMap.get( trackDesc ).get
    		 Some(track)
    	 } else {
    		 val trackOpt = generateTrack( trackDesc)
    		 info(s" Generating track again $trackDesc ; Found $trackOpt ") 
    		 trackOpt match {
    		   case Some(track) =>
    		     _trackMap.put( trackDesc, track)
    		     Some(track)   
    		   case None => None
    		 }
    	 }
     } else {
       getLatestTrack( trackDesc)
     }
   }
}

object TrackFactory {
    class  TracksUnavailableException( reason : Throwable) extends RuntimeException(reason)
    
  
}