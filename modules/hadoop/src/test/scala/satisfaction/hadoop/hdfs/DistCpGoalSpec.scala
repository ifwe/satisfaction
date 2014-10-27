package satisfaction
package hadoop
package hdfs

import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level
import satisfaction.engine.Satisfaction
import satisfaction.fs.FileSystem
import hive.ms.MetaStore


/**
@RunWith(classOf[JUnitRunner])
class DistCpGoalSpec extends Specification {
  
  
    implicit  val ms : MetaStore = MetaStore(new java.net.URI("thrift://jobs-dev-sched2:9093"))
    implicit  val hdfs : FileSystem = new Hdfs("hdfs://jobs-dev-hnn" )
    
    implicit val track : Track = new Track(TrackDescriptor("TestTrack"))
   

    "DistCpSpec" should {
        "DistCp a file from prod to dev" in {
            val srcPath = new VariablePath("${srcNameNode}${twFriendsSrcDir}/${dateString}/output")
            val destPath = new VariablePath("${nameNode}/${twFriendsDestDir}/${dateString}/output")
            val distCpAction = DistCpGoal("DistCp Twitter", srcPath, destPath)

            val runDate = Variable("dateString")
            val witness = Witness((runDate -> "20130917"))
            ///val result = engine.satisfyProject(project, witness)
            val goalResult = Satisfaction.satisfyGoal(distCpAction, witness)
            
            goalResult.state == GoalState.Success
        }

    }
}
**/