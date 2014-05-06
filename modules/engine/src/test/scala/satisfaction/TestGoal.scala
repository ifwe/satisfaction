package com.klout
package satisfaction;
package engine
package actors


object TestGoal {

    def apply(name: String, variables: Set[Variable[_]])( implicit track : Track): Goal = {

        val satisfier = new MockSatisfier()
        val evidence = Set[Evidence](satisfier)
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, dependencies, evidence)

        goal

    }
    

    def SlowGoal(name: String, variables: Set[Variable[_]], progressCount: Int, sleepTime: Long)(implicit track: Track): Goal = {

        val satisfier = new SlowSatisfier(progressCount, sleepTime)
        val evidence = Set[Evidence](satisfier)
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables,  dependencies, evidence)

        goal

    }
    
    def AlreadySatisfiedGoal(name: String, variables: Set[Variable[_]], progressCount: Int, sleepTime: Long)(implicit track:Track): Goal = {

        val satisfier = new SlowSatisfier(progressCount, sleepTime)
        satisfier.varMap = satisfier.varMap ++ variables.map( _.name )
        val evidence = Set[Evidence](satisfier)
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, dependencies, evidence)

        goal

    }


    def FailedGoal(name: String, variables: Set[Variable[_]], progressCount: Int, sleepTime: Long)(implicit track:Track): Goal = {

        val satisfier = new SlowSatisfier(progressCount, sleepTime)
        satisfier.retCode = false
        val evidence = Set[Evidence](satisfier)
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, dependencies, evidence)

        goal
    }

}