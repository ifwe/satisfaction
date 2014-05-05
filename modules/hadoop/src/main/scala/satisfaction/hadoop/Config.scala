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
        
        //// 
        
        ///hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-dev-sched2:9085")
        ///hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-dev-sched2:9083")
        ///hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-aa-sched1:9083")
        ///hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:9083")
        hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://dhdp2jump01:9083")
        
        
        /// XXX How to use play/scala configuration
        hc.set("mapreduce.framework.name", "yarn")
        ///hc.set("mapreduce.jobtracker.address", "jobs-dev-hnn:8021")
        ///hc.set("mapred.job.tracker", "jobs-dev-hnn1:8021")
        
         /// XXX fix me ...
        ///hc.set("fs.default.name", "hdfs://jobs-dev-hnn1:8020")
        ///hc.set("fs.default.name", "hdfs://nameservice1")
        
        
         /// Some properties we may need to set ...
        hc.set("mapred.child.java.opts", " -Xmx2048m ")
        ///hc.set("dfs.nameservices", "hdfs://jobs-dev-hnn1")
        hc.set("yarn.resourcemanager.address", "scr@wyoucloudera")
        hc.set("hive.stats.autogather","false")
        
        
        System.out.println(" HADOOP CONFIG ");
        hc.logVars(System.out)


        return hc
    }
    
    

    val config = initHiveConf

}