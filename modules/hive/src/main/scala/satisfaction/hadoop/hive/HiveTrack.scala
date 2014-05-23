package com.klout
package satisfaction
package hadoop
package hive

import satisfaction.Track
import ms.MetaStore

class HiveTrack(tr : TrackDescriptor) extends Track(tr) {
  
     /// XXX JDB -- Can we better handle implicits ???
     //// XXX Make a Hadoop Track which contains the implicit Metastore
     implicit lazy val ms  : MetaStore  = new MetaStore(Config(this)) //// Will we 
  

}