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
     
     
  } 
     
     
}