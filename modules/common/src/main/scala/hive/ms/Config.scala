package hive.ms

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.conf.Configuration

/**
 *  Scala Object to handle initial configuration
 *   to be used
 */
object Config {
    def initHiveConf: HiveConf = {
        print(ShimLoader.getMajorVersion())
        var hc = new HiveConf(new Configuration(), this.getClass())
        hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-dev-sched2:9083")

        return hc
    }
    
    

    val config = initHiveConf

}