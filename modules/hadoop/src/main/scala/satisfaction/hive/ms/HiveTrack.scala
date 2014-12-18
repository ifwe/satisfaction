package satisfaction
package hadoop
package hive.ms

import satisfaction.Track
import satisfaction.TrackDescriptor
import satisfaction.fs.Path
import satisfaction.track.TrackFactory
import satisfaction.track.TrackLoader
import satisfaction.track.DefaultTrackLoader

/**
 *  Allow the track to access the currently configured Hive MetaStore
 *  by extending HiveTrack, instead of just Track
 *  
 */
class HiveTrack(tr : TrackDescriptor)(implicit var ms : MetaStore = MetaStore.default) extends Track(tr) with Logging {
  
  
  

}

