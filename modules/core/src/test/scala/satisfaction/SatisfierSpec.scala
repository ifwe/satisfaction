package com.klout
package satisfaction


import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class SatisfierSpec extends Specification {

  
    
    class RobustSatisfier extends Satisfier {
    
        def name = "Robustly"
      
        @Override
        override def satisfy( subst:Substitution) : ExecutionResult = robustly  {
             println(" Calling a Boolean ")
             println( " Substitution is "  + subst)
              true
        }
        
        
        @Override
        override def abort() : ExecutionResult = { null}
      
    }
    
    class BarfingSatisfier extends Satisfier {
        def name = "Barfing" 

        @Override
        override def satisfy( subst:Substitution) : ExecutionResult = robustly  {
      
          println( " Substitution is "  + subst)
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
        
        val subst = Substitution( VariableAssignment[Int]("A" , 2), VariableAssignment[Int]("B" ,5))
        val execResult = robustSatisfier.satisfy( subst)
        
        println( execResult)
       
     }
     
     "barf result" in {
        val barfSatisfier = new BarfingSatisfier();
        
        val subst = Substitution( VariableAssignment[Int]("A" , 2), VariableAssignment[Int]("B" ,5))
        val execResult = barfSatisfier.satisfy( subst)
        
        println( execResult)
       
     }
     
     
     
     
     
     
}