package com.klout.satisfaction;
package executor
package actors

object TestGoal {

    def apply(name: String, variables: Set[Param[_]]): Goal = {

        val satisfier = new MockSatisfier()
        val evidence = Set[Evidence](satisfier)
        val overrides = null
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, overrides, dependencies, evidence)

        goal

    }

    def SlowGoal(name: String, variables: Set[Param[_]], progressCount: Int, sleepTime: Long): Goal = {

        val satisfier = new SlowSatisfier(progressCount, sleepTime)
        val evidence = Set[Evidence](satisfier)
        val overrides = null
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, overrides, dependencies, evidence)

        goal

    }

    def FailedGoal(name: String, variables: Set[Param[_]], progressCount: Int, sleepTime: Long): Goal = {

        val satisfier = new SlowSatisfier(progressCount, sleepTime)
        satisfier.retCode = false
        val evidence = Set[Evidence](satisfier)
        val overrides = null
        val dependencies = Set[(Witness => Witness, Goal)]()

        val goal = new Goal(name, Some(satisfier), variables, overrides, dependencies, evidence)

        goal
    }

}
