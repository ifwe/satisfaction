package satisfaction
package hadoop
package hive.ms

import org.specs2.mutable._
import satisfaction.Witness
import satisfaction._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import satisfaction.fs.FileSystem
import satisfaction.hadoop.hdfs.Hdfs
import satisfaction.fs.LocalFileSystem
import satisfaction.fs.Path

@RunWith(classOf[JUnitRunner])
class HiveTableSpec extends Specification {
    val dtParam = new Variable("dt", classOf[String])
    val dateParam = new Variable("date", classOf[String])
    val hourParam = new Variable("hour", classOf[String])
    val networkParam = new Variable("network_abbr", classOf[String])
    val featureGroup = new Variable[Int]("service_id", classOf[Int])

    //// XXX Externalize  ---
    //// Remove references to Klout ( or Tagged)
    ///implicit  val ms : MetaStore = MetaStore(new java.net.URI("thrift://dhdp2jump01:9083"))
    implicit val ms : MetaStore = MetaStore.default
    implicit  val hdfs : FileSystem = Hdfs.default
    implicit val track : Track = Track.localTrack( "PartitionExistsTrack", 
           LocalFileSystem.relativePath( new Path(
                "modules/hadoop/src/test/resources/track/PartitionExists")))
    
    "HiveTable" should {
      /**
        "provide variables" in {
            val dauByPlatform = new HiveTable("ramblas", "dau_by_platform")
            val params = dauByPlatform.variables
            val dtVar = Variable[String]("dt", classOf[String])
            params.foreach(p =>
                println (" Parameter is " + p.name)
            )

            params.size must_== 2
            params must contain(Variable[String]("dt", classOf[String]))
            params must contain(Variable[String]("hour", classOf[String]))
        }
        "implements exists" in {
            val dauByPlatform = new HiveTable("sqoop_test", "dau_by_platform")
            val witness = new Witness(Set((dtParam -> "20140420"),
                (hourParam -> "07")))
            

            val xist = dauByPlatform.exists(witness)
            if (xist) {
                println("  Witness exists ")
            } else {
                println(" Witness doesn't exist")
            }

            xist must_== true
        }
        "partitiion doesnt exists" in {
            val dauByPlatform = new HiveTable("ramblas", "dau_by_platform")
            val witness = new Witness(Set((dtParam -> "20190821"), (hourParam -> "05")))

            val doesNotExist = dauByPlatform.exists(witness)

            doesNotExist must_== false
        }
        "traditional get Partition" in {
            val pageViewLog = new HiveTable("sqoop_test", "page_view_log")
            val witness = new Witness(Set((dateParam -> "20140518"), (hourParam -> "05")))
            
            val partOpt = pageViewLog.getPartition( witness)

            partOpt match {
              case Some(p) => println(" Partition is " + p)

              case None => println(" No partition")
            }
            
            ///partOpt must be ('defined)
            partOpt.isDefined must_== true
            ///partOpt must be (None)
        }
        "create partition" in {
            val pageViewLog = new HiveTable("sqoop_test", "page_view_log")
            val witness = new Witness(Set((dateParam -> "20140615"), (hourParam -> "05")))
            
            val partOpt = pageViewLog.getPartition( witness)

            partOpt match {
              case Some(p) => println(" Partition is " + p)

              case None => println(" No partition")
            }
            
            ///partOpt must be (None)
            
            val newPart = pageViewLog.addPartition(witness)
            
            
            val partOpt2 = pageViewLog.getPartition( witness)
            
            
            true
        }
        **/
        "non partitoned table" in {
            val chkConfig = Config.config 
            val failOver = chkConfig.get( "dfs.client.failover.proxy.provider.dhdp2")
            println(" FailOver Provider = " + failOver)
            failOver must not beNull
          
          
            val pageViewLogNonPartitioned = new HiveTable("default", "page_view_event_non_partitioned", false)
            
            val emptyWitness = new Witness(Set.empty)
            
            val exists = pageViewLogNonPartitioned.exists( emptyWitness)
            if( exists) {
              println("XXX XXXX non Partitoned table exists")
            } else {
              println("XXX XXXXnon Partitoned table Doesn't  exists")
            }
            
            exists must_== true

            val dataInstanceOpt : Option[DataInstance] = pageViewLogNonPartitioned.getDataInstance( emptyWitness)
            
            dataInstanceOpt.isDefined must_== true
            
            val dataInstance = dataInstanceOpt.get
            
            println("  XXX XXX XXX Non Partitioned data instance is " + dataInstance)
            System.out.println("  XXX XXX XXX Non Partitioned data instance is " + dataInstance)
            
            dataInstance.exists must_== true
            
            println(" Created time = " + dataInstance.created )

          
        }

    }


}