package com.klout
package satisfaction
package hadoop
package hive

import satisfaction.Track
import ms.MetaStore

class HiveTrack(tr : TrackDescriptor) extends Track(tr) {
  
     implicit lazy val ms  : MetaStore  = new MetaStore(Config(this)) 
  
     implicit val hiveTrack : Track = this

}