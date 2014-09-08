package satisfaction


import org.specs2.mutable._
import org.junit.runner.RunWith
import Temporal._
import org.specs2.runner.JUnitRunner
import satisfaction.retry.Retryable
import satisfaction.retry.Retryable._
import satisfaction.Evidence

@RunWith(classOf[JUnitRunner])
class RetryableSpec extends Specification {

  implicit val track : Track = {
      val tf = Track( TrackDescriptor("Retryable"))
      val props = new java.util.Properties
      props.put("satisfaction.retry.numRetries", "3")
      tf.setTrackProperties( props)
      
      tf
  }
  
  "Retryable" should {
  
    
     "modify existing goal" in {
         val sampleGoal = Goal.RunThis("SimpleGoal" , { witness => {
            println(" Booger")  
            true
         } } )
         
         
         ///val anotherGoal = sampleGoal ::
         
         val anotherGoal = new RetryableGoal(sampleGoal)
         
         
         anotherGoal match {
           case  r : Retryable => println(" Yeah I'm retryable ") 
           case _  => println( "Boo Im not !!")
         }
         
         
         val topLevel = anotherGoal.declareTopLevel

         topLevel match {
           case  r : Retryable => println(" Yeah I'm retryable ") 
           case _  => println( "Boo Im not !!")
         }
         
         val addDep = topLevel.addEvidence( Evidence.NeverSatisfied )
         addDep match {
           case  r : Retryable => println(" Yeah I'm retryable ") 
           case _  => println( "Boo Im not !!")
         }
         

         
         
     }
     
     
  } 
     
     
}