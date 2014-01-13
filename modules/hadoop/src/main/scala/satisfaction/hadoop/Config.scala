package com.klout
package satisfaction
package hadoop

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
        hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-dev-hive2:9085")
        ///hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-aa-sched1:9083")
        
        
        /// XXX How to use play/scala configuration
        hc.set("mapreduce.framework.name", "classic")
        hc.set("mapreduce.jobtracker.address", "jobs-dev-hnn:8021")
        hc.set("mapred.job.tracker", "jobs-dev-hnn1:8021")
        hc.set("yarn.resourcemanager.address", "scr@wyoucloudera")


        return hc
    }
    
    

    val config = initHiveConf

}