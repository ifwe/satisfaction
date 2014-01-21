package com.klout
package satisfaction
package hadoop
package hive

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.klout.satisfaction.MetricsProducing
import org.apache.hadoop.hive.conf.HiveConf

@RunWith(classOf[JUnitRunner])
class HiveDriverSpec extends Specification {
  
  
   /// XXX Pass in metastore and hdfs urls
  implicit val hiveConf : HiveConf = clientConfig
  
  
  def clientConfig : HiveConf = {
      val conf = Config.config
      val testPath = System.getProperty("user.dir") + "/modules/hadoop/src/test/resources/config/hdfs-site.xml"
      conf.addResource( new java.io.File(testPath).toURI().toURL())
      
      
       val nameService = conf.get("dfs.nameservices")
       if(nameService != null) {
         conf.set("fs.defaultFS", s"hdfs://$nameService")
       }
      conf
  }
  
  /**
  implicit val hiveConf : HiveConf = {
      val hc =  Config.config
      
             //// 
        ///hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-aa-sched1:9085")
        hc.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://jobs-aa-sched1:9083")
        
        
        /// XXX How to use play/scala configuration
        hc.set("mapreduce.framework.name", "classic")
        hc.set("mapreduce.jobtracker.address", "jobs-aa-hnn:8021")
        hc.set("mapred.job.tracker", "jobs-aa-hnn:8021")
        hc.set("fs.default.name", "hdfs://jobs-aa-hnn:8020")
        hc.set("dfs.nameservices", "hdfs://jobs-aa-hnn")
        hc.set("yarn.resourcemanager.address", "scr@wyoucloudera")

        hc
  }
  * 
  */

      /**
    "DriverTest" should {
        "access hive client " in {

            ///val log = Logger.getLogger( classOf[org.apache.hadoop.mapreduce.Cluster])
            val log = Logger.getLogger("org.apache.hadoop")
            log.setLevel(Level.DEBUG)

            val showDB = HiveClient.executeQuery("show databases")
            val weird = HiveClient.executeQuery("SET yarn.resourcemanager.address=\"foobar\";")
            HiveClient.useDatabase("bi_maxwell")
            val showTbls = "show tables"
            val out1 = HiveClient.executeQuery(showTbls)
            val query = " create table blah as select * from bi_maxwell.actor_action where dt='20130812' and network_abbr='tw' "

            val output = HiveClient.executeQuery(query)
            println(" Output is " + output)
        }
    }
        **/
  
   /** 
    "LocalDriverTest" should {
        println(" BOOGER ") 
        val hiveDriver = HiveDriver("/Users/jeromebanks/NewGit/satisfaction/auxlib")
        println(" BUBBA ") 
        hiveDriver.useDatabase("bi_maxwell")
        
        val createFactContentHQL = "create external table if not exists raw_content" +
            " partitioned by (dt string, network_abbr string) " +
            " row format serde 'com.inadco.ecoadapters.hive.ProtoSerDe' " +
            " with serdeproperties( \"messageClass\"=\"com.klout.platform.protos.Topics$FactContent\") " +
            " stored as sequencefile " +
            " location '/data/hive/maxwell/raw_content'  "

      
          hiveDriver.executeQuery(createFactContentHQL)
      
    }
  
    "execute query" should {
        println(" BOOGER ") 
        val hiveDriver = HiveDriver("/Users/jeromebanks/NewGit/satisfaction/auxlib")
        println(" BUBBA ") 
        hiveDriver.useDatabase("bi_maxwell")
        
        val queryActorActionHQL = "select network_abbr, count(*) as net_count " +
            " from actor_action " +
            " where dt=20140116 " +
            " group by network_abbr" ;

      
        //// XXX  add api to read rows 
        hiveDriver.executeQuery(queryActorActionHQL)
    }
    * **
    */
  
    "create view and table" should {
        println(" BOOGER ") 
        val hiveDriver = HiveDriver(System.getProperty("user.dir") + "/auxlib")
            
        println(" BUBBA ") 
        hiveDriver.useDatabase("bi_maxwell")
        
        val dropViewHQL = "drop view if exists jdb_blah_view"
        hiveDriver.executeQuery( dropViewHQL)  
        
        
        val createViewHQL = "create view jdb_blah_view as " +
            "select network_abbr, count(*) as net_count " +
            " from actor_action " +
            " where dt=20140116 " +
            " group by network_abbr" ;

        hiveDriver.executeQuery(createViewHQL)
        
        val dropTableHQL = "drop table if exists jdb_blah "
        hiveDriver.executeQuery(dropTableHQL)
        
        val createTableHQL = " create table jdb_blah " +
        " as select * from jdb_blah_view ";
      
        hiveDriver.executeQuery(createTableHQL)
    }
  
    /**
    
    "error out " should {
        val hiveDriver = HiveDriver("/Users/jeromebanks/NewGit/satisfaction/auxlib")
        hiveDriver.useDatabase("bi_maxwell")
        val bogusString = " dsfadfksadf jkasdfnlka;sfd asd "
          
        val result=   hiveDriver.executeQuery(bogusString)
      
        println(" Resultis " + result)
        
        result mustEqual false
    }
    
    "missing UDF" should {
        val hiveDriver = HiveDriver("/Users/jeromebanks/NewGit/satisfaction/auxlib")
        hiveDriver.useDatabase("bi_maxwell")
        val bogusString = " CREATE TEMPORARY FUNCTION bozo_udf AS 'org.clown.BozoUDF' "
          
        val result=   hiveDriver.executeQuery(bogusString)
      
        println(" Resultis " + result)
        
        result mustEqual false
    }
    
    "missing Table" should {
        val hiveDriver = HiveDriver("/Users/jeromebanks/NewGit/satisfaction/auxlib")
        hiveDriver.useDatabase("bi_maxwell")
        val bogusString = " create table missing as select * from missing_in_action "
          
        val result=   hiveDriver.executeQuery(bogusString)
      
        println(" Resultis " + result)
        
        result mustEqual false
    }
    
    "get metrics" should {
        val hiveDriver = HiveDriver("/Users/jeromebanks/NewGit/satisfaction/auxlib")
        hiveDriver.useDatabase("bi_maxwell")
        
        val countActorAction = "select network_abbr, count(*) as fact_count " +
          " from actor_action " +
          " where dt= '20131125' " +
          " group by network_abbr "
          
        val result=   hiveDriver.executeQuery(countActorAction)
      
        println(" Resultis " + result)
        
        val mp = hiveDriver.asInstanceOf[MetricsProducing]
        val metrics = mp.jobMetrics
        
        println(s" Metrics is ${metrics.collectionName}")
        
        metrics.metrics.foreach { case (metric, value)  => {
           println(s" Metric $metric = $value ")
          }
        }
    }
    * **/
    

}