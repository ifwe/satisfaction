package com.klout
package satisfaction
package hadoop
package hive

import satisfaction.Track
import ms.MetaStore

/**
 *  Allow the track to access the currently configured Hive MetaStore
 *  by extending HiveTrack, instead of just Track
 *  
 */
class HiveTrack(tr : TrackDescriptor)(implicit var ms : MetaStore = MetaStore.default) extends Track(tr) {
  

}