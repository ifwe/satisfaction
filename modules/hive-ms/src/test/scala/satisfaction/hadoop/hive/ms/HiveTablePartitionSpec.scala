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
class HiveTablePartitionSpec extends Specification {
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
      

        "traditional mark Partition complete" in {
          
            val pageViewLog = new HiveTable("ramblas", "user_event_sessions")
            val witness = new Witness(Set((dtParam -> "20140822"), (hourParam -> "13")))
            
            val partOpt = pageViewLog.getPartition( witness)

            partOpt match {
              case Some(p) => println(" Partition is " + p)

              case None => println(" No partition")
            }
            
            ///partOpt must be ('defined)
            partOpt.isDefined must_== true
            ///partOpt must be (None)

            val partition = partOpt.get
            partition.markCompleted
            assert(partition.isMarkedCompleted)
        }


    }


}