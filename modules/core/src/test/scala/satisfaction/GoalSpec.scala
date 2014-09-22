package satisfaction

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import satisfaction.fs.FileSystem
import satisfaction.fs.LocalFileSystem
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GoalSpec extends Specification {

    
  case class SimpleDataOutput(
      val variables: List[Variable[_]] = List.empty
     ) extends DataOutput {
    
    def exists(witness: Witness): Boolean = {
       false;
    }
    
    def getDataInstance(witness: Witness): Option[DataInstance] = {
      None
    }
    
  }
     
     "Add Evidence" in {
       implicit val track : Track = new Track(TrackDescriptor("TestTrack"))
       val goal : Goal = new Goal(name="TestGoal",satisfier=None)
       
       
       goal.addEvidence( new SimpleDataOutput)
       
       
       true
     }
     
     
     "FanOut Goal" in {
         implicit val track : Track = new Track(TrackDescriptor("TestTrack"))
         
         val subGoal : Goal = DataDependency( new SimpleDataOutput( List( Variable("date"), Variable("hour"))) )

         val fanOut: Goal = FanOutGoal( subGoal, Variable("hour") ,   (0 to 23).map( _.toString )  )
         
         
         fanOut.dependencies.foreach( dep => {
             println(dep._2.name) 
         })
       
     }
     

     
}