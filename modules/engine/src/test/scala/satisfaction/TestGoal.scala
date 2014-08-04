package satisfaction;
package engine
package actors

import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat


object TestGoal {

    def apply(name: String, variables: List[Variable[_]])( implicit track : Track): Goal = {

        val satisfier = new MockSatisfier()
        val evidence = Set[Evidence](satisfier)
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, dependencies, evidence)

        goal

    }
    

    def SlowGoal(name: String, variables: List[Variable[_]], progressCount: Int, sleepTime: Long)(implicit track: Track): Goal = {

        val satisfier = new SlowSatisfier(progressCount, sleepTime)
        val evidence = Set[Evidence](satisfier)
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables,  dependencies, evidence)

        goal

    }
    
    def AlreadySatisfiedGoal(name: String, variables: List[Variable[_]], progressCount: Int, sleepTime: Long)(implicit track:Track): Goal = {

        val satisfier = new SlowSatisfier(progressCount, sleepTime)
        satisfier.varMap = satisfier.varMap ++ variables.map( _.name )
        val evidence = Set[Evidence](satisfier)
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, dependencies, evidence)

        goal

    }


    def FailedGoal(name: String, variables: List[Variable[_]], progressCount: Int, sleepTime: Long)(implicit track:Track): Goal = {

        val satisfier = new SlowSatisfier(progressCount, sleepTime)
        satisfier.retCode = false
        val evidence = Set[Evidence](satisfier)
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, dependencies, evidence)

        goal
    }
    
    
    class DailySatisfier( dtStr: String ) extends Satisfier  with Evidence {
        def name = s"Daily Historical to $dtStr"

        val dtVar =  Variable("dt")
        val formatter = DateTimeFormat.forPattern("YYYYMMdd")
        
        val dtLowest = formatter.parseDateTime(dtStr)
      
        override def satisfy( w : Witness ) : ExecutionResult = robustly {
          val thisYear = w.get(dtVar).get
          println(s" Satisfying for the year $thisYear ");
          Thread.sleep( 5000)
          
          true
        }
        
        override def exists( w: Witness ) : Boolean = {
          println(" W is " + w)
          val dtWitness = formatter.parseDateTime( w.get( dtVar).get )
          dtWitness.isBefore( dtLowest)  || dtWitness.isEqual( dtLowest)
        }
      
      
        override def abort() = { null }
    }
    
    def ReharvestGoal( dtLowest : String )(implicit track: Track) : Goal = {
       val daily = new DailySatisfier( dtLowest)
        new Goal(name = s" Reharvest to $dtLowest",
            satisfier = Some( new DailySatisfier(dtLowest)),
            variables = List( Variable("dt")),
            evidence = Set( daily)).reharvestDaily
    }

}
