package satisfaction


import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class SatisfierSpec extends Specification {

  
    
    class RobustSatisfier extends Satisfier {
    
        def name = "Robustly"
      
        @Override
        override def satisfy( witness:Witness) : ExecutionResult = robustly  {
             println(" Calling a Boolean ")
             println( " Witness is "  + witness)
              true
        }
        
        
        @Override
        override def abort() : ExecutionResult = { null}
      
    }
    
    class BarfingSatisfier extends Satisfier {
        def name = "Barfing" 

        @Override
        override def satisfy( witness:Witness) : ExecutionResult = robustly  {
      
          println( " Witness is "  + witness)
          println(" I think I'm going to barf !!!!")
          throw new RuntimeException(" Get me a bucket" )
        }

        @Override
        override def abort() : ExecutionResult = { null}
    }
     
     
     "RobustRunCorrectly" in {
        RobustRun("DD",   {
          true 
        })

        true
     }
     
     
     "robustly qualifer" in {
        val robustSatisfier = new RobustSatisfier();
        
        val witness = Witness( VariableAssignment[Int]("A" , 2), VariableAssignment[Int]("B" ,5))
        val execResult = robustSatisfier.satisfy( witness)
        
        println( execResult)
       
     }
     
     "barf result" in {
        val barfSatisfier = new BarfingSatisfier();
        
        val witness = Witness( VariableAssignment[Int]("A" , 2), VariableAssignment[Int]("B" ,5))
        val execResult = barfSatisfier.satisfy( witness)
        
        println( execResult)
       
     }
     
     
     
     
     
     
}