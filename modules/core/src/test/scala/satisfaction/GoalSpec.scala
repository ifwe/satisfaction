package com.klout
package satisfaction

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class GoalSpec extends Specification {

    
  case class SimpleDataOutput extends DataOutput {
    def variables: Set[Variable[_]] = Set.empty
    
    def exists(witness: Witness): Boolean = {
       false;
    }
    
    def getDataInstance(witness: Witness): Option[DataInstance] = {
      None
    }
    
  }
     
     "Add Evidence" in {
       implicit val track : Track = new Track(TrackDescriptor("TestTrack"), Set.empty)
       val goal : Goal = new Goal(name="TestGoal",satisfier=None)
       
       
       goal.addEvidence( new SimpleDataOutput)
       
       
       true
     }
     
     
}