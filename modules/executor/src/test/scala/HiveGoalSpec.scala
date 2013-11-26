package com.klout.satisfaction

import scalaxb._
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.klout.satisfaction.executor.actors.ProofEngine
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class HiveGoalSpec extends Specification {
    val NetworkAbbr = new Variable[String]("network_abbr", classOf[String])
    val DoDistcp = new Variable[Boolean]("doDistcp", classOf[Boolean])
    val runDate = new Variable[String]("dt", classOf[String])

    
    
    "HiveGoalTest" should {
      val rawContentDir = new VariablePath("${nameNode}/data/hive/maxwell/raw_content/${dateString}/${networkAbbr}")
      val actorActionTable = new HiveTable("bi_maxwell", "actor_action")
      val hiveGoal = HiveGoal("fact_content", "fact_content.hql", actorActionTable, None, Set.empty )
      ////hiveGoal.addDataDependency( rawContentDir )
          
      
      val td = TrackDescriptor("actor_action")
      val hiveTrack = new Track(td, Set(hiveGoal))
      hiveTrack.readProperties("maxwell.properties")
      hiveTrack.registerJars( "/Users/jeromebanks/NewGit/satisfaction/auxlib");
      
      val engine = new ProofEngine
      val witness = Witness((runDate -> "20131125"), (NetworkAbbr -> "tw"))
      val result = engine.satisfyGoalBlocking( hiveTrack, hiveGoal, witness, Duration( 30, MINUTES)  )
    }

}