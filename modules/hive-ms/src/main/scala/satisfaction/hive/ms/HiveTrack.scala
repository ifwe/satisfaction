package satisfaction
package hadoop
package hive.ms

import satisfaction.Track
import satisfaction.TrackDescriptor
import satisfaction.fs.Path
import org.apache.hadoop.hive.conf.HiveConf

/**
 *  Allow the track to access the currently configured Hive MetaStore
 *  by extending HiveTrack, instead of just Track
 *  
 */
class HiveTrack(tr : TrackDescriptor) extends Track(tr) with Logging {
  
    implicit lazy val hiveConf = createHiveConf() 
    implicit lazy val ms : MetaStore = createMetaStore()
    
    
    def createHiveConf() : HiveConf = {
       val hc = new HiveConf( Config(track), this.getClass() ) 
       hc.setClassLoader( this.getClass().getClassLoader )
       hc
    }
    
    def createMetaStore() : MetaStore = {
        new MetaStore()(hiveConf)
    }

}

