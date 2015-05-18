package satisfaction


import org.specs2.mutable._
import org.junit.runner.RunWith
import Temporal._
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TemporalSpec extends Specification {

  
  "TemporalVariables" should {
    
     "Produce hourly ranges" in {
       Temporal.hours foreach println
       
       val hourSet = hours.toSet
       
       hourSet.size must_== 24
     }
     
     "Produce time ranges" in {
       
        Temporal.dateRange( "20140401", "20140601") foreach println
        
        val days = dateRange( "20140401", "20140601").toList
        println(" Size is " + days.size)
        
        days.take(1)(0)  must_==  "20140401"

        days.takeRight(1)(0)  must_==  "20140531"
          
        days.size must_!=  0
          
     }
     
     
    /**
     *  Validate that the witness contains all the variables needed for the goal
     *   XXX move to utility
     */
    def checkVariables( goal : Goal, wit : Witness ) : Boolean = {
       goal.variables.foreach( v => {
          if(!wit.contains(v)) {
              return false
          }
       })
       true
    }

     "Be equivalent to String vars" in {
         val temporalWitness = Witness( TemporalVariable.Dt -> "20150223")
         
         val strVar = Variable("dt")
       
         temporalWitness.contains( strVar)  must_== true
         
         val strWitness = Witness( strVar -> "20150223")
         
         System.out.println(s"  TemporalContains = ${temporalWitness.contains(strVar)} ")
         System.out.println(s"  StrContains = ${strWitness.contains(TemporalVariable.Dt)} ")
         
         strWitness.contains( TemporalVariable.Dt )  must_== true
     }
     
     "TemporalVariable.isTemporal" in {
         import TemporalVariable._
         TemporalVariable.isTemporalVariable( Variable("hour") ) match {
           case Some(hour) => {  hour == TemporalVariable.Hour }
           case None => false
         }
     }
     
  } 
     
     
}