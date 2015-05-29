package com.klout.satisfaction
package executor
package actors

import scalaxb._
import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import com.klout.klout_scoozie.common.Network
import com.klout.klout_scoozie.common.Networks
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level

@RunWith(classOf[JUnitRunner])
class DistCpGoalSpec extends Specification {

    "DistCpSpec" should {
        "DistCp a file from prod to dev" in {
            val engine = new ProofEngine()
            val srcPath = new VariablePath("${srcNameNode}${twFriendsSrcDir}/${dateString}/output")
            val destPath = new VariablePath("${nameNode}/${twFriendsDestDir}/${dateString}/output")
            val distCpAction = DistCpGoal("DistCp Twitter", srcPath, destPath)

            val runDate = Variable("dateString")
            val witness = Witness((runDate -> "20130917"))
            ///val result = engine.satisfyProject(project, witness)
            Satisfaction.satisfyGoal(distCpAction, witness)
        }

    }
}