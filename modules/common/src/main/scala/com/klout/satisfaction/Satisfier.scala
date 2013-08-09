package com.klout.satisfaction

abstract trait Satisfier {

    def satisfyGoal(goal: Goal, goalPeriod: Witness)
    def isSatisfied(goal: Goal, goalPeriod: Witness): Boolean
}

object Satisfier {
    implicit val fmt = ???
}
