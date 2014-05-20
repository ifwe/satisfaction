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
import java.io.File
import satisfaction.fs.Path
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

@RunWith(classOf[JUnitRunner])
class HiveDriverSpec extends Specification {
  
  
   /// XXX Pass in metastore and hdfs urls
   implicit val track : Track =  {
      val tr = new Track(TrackDescriptor("HiveLocalDriver"))
      tr.setAuxJarFolder( new File("./auxlib")) /// XXX Ref to File, not Path
      
      tr.setTrackPath( new Path( System.getProperty("user.dir") + "/modules/hive/src/test"))
      
      
      tr
   }
   
   implicit val hiveConf : HiveConf = Config( track)
  
  
  

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
   /**

    " HADOOP_HOME env var works " should {
     
      val hc = Config.config
      hc.logVars(System.out)
      val hiveMS = hc.getVar( ConfVars.METASTOREURIS)
      
      println(s" Hive MetaStore = $hiveMS")
      
      
      hiveMS must_!= null
      
   }
   * 
   */
   
   
  
    " Implicitly load configuration from Test Track " should {
     
      val hc = hiveConf
      hc.logVars(System.out)
      val hiveMS = hc.getVar( ConfVars.METASTOREURIS)
      
      println(s" Hive MetaStore = $hiveMS")
      
      
      hiveMS must_!= null
      
   }
   
    /**
   
    "create view and table" should {
        
        
        println(" BOOGER ") 
        val hiveDriver = new HiveLocalDriver
            
        println(" BUBBA ") 
        val res1 = hiveDriver.useDatabase("sqoop_test")
        println(s" RES1 = $res1")
        res1 must_== true
        
        val dropViewHQL = "drop view if exists dau_platform_view"
        hiveDriver.executeQuery( dropViewHQL)  must_== true
        
        
        val createViewHQL = " CREATE VIEW dau_platform_view( " +  "\n" +
     "   user_id," + "\n" +
     "   platform," + "\n" +
     "   source)" + "\n" +
     "AS" + "\n" +
     "  SELECT user_id," + "\n" +
     "    'Android' as platform," + "\n" +
     "    1 as source" + "\n" +
     "  FROM login_detail_log" + "\n" +
     "  WHERE" + "\n" +
     "    user_id >0" + "\n" +
     "  AND date = '20140421'" + "\n" +
     "  AND hour = '23'" + "\n" +
     "  AND (lower(ua) like 'dalvik%'" + "\n" +
     "  OR  lower(ua) like 'tagged/%/an%'" + "\n" +
     "  OR lower(ua) like 'hi5/%/an%')" + "\n" +
     "UNION ALL" + "\n" +
     "  SELECT  user_id," + "\n" +
     "    if(lower(domain) in ('tagged.com', 'hi5.com'), 'Web', 'MobileWeb') as platform," + "\n" +
     "    2 as source" + "\n" +
     "  FROM page_view_log" + "\n" +
     "  WHERE user_id >0" + "\n" +
     "  AND date = '20140421'" + "\n" +
     "  AND hour = '23'" + "\n" +
     "  AND is_redir!='t'" + "\n" +
     "  AND lower(domain) in ('tagged.com', 'hi5.com', 'm.tagged.com', 'm.hi5.com')" + "\n" +
     "UNION ALL" + "\n" +
     "  SELECT user_id," + "\n" +
     "   if(lower(mobile_type) LIKE 'tagged/%/an+%'" + "\n" +
     "     OR  lower(mobile_type) like 'hi5/%/an+%'" + "\n" +
     "     OR lower(mobile_type) = 'tagged/2.0+cfnetwork/459 darwin/10.0.0d3'" + "\n" +
     "     OR lower(mobile_type) like 'dalvik%'" + "\n" +
     "     OR lower(mobile_type) = '' ,'Android','iPhone') as platform," + "\n" +
     "   3 as source" + "\n" +
     "  FROM mobile_api_log_summary" + "\n" +
     "  WHERE user_id >0" + "\n" +
     "  AND date = '20140421'" + "\n" +
     "  AND hour = '23'" + "\n" +
     "UNION ALL" + "\n" +
     "  SELECT user_id," + "\n" +
     "    if(lower(ua) LIKE 'tagged/%/an+%'" + "\n" +
     "       OR lower(ua) LIKE 'hi5/%/an+%'" + "\n" +
     "  OR lower(ua) = 'tagged/2.0+cfnetwork/459 darwin/10.0.0d3'" + "\n" +
     "  OR lower(ua) like 'dalvik%' OR lower(ua) = '' OR lower(ua) like 'mozilla%', 'Android','iPhone') as platform, " + "\n" +
     " 4 as source " + "\n" +
     " FROM mobile_app_event_log " + "\n" +
     " WHERE user_id > 0 " + "\n" +
     " AND date = '20140421' " + "\n" +
     " AND hour = '23' " + "\n" +
     " and type in ('autologin', 'login', 'open') " 

        hiveDriver.executeQuery(createViewHQL)   must_== true
        
       val insertQuery = " INSERT OVERWRITE TABLE  dau_by_platform PARTITION(dt='20140421',hour='23') " + "\n" +
    " SELECT" + "\n" +
    "   platform_id," + "\n" +
    "   count(*) as user_count" + "\n" +
    "  FROM dau_user_platform_view" + "\n" +
    "  GROUP BY platform_id" 
    
          
     hiveDriver.executeQuery(insertQuery) must_== true
    
    
       }  
       *  
       */
    /** +
    
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