package com.klout.satisfaction

import hive.ms._

class HiveSatisfier( ms : MetaStore)  extends Satisfier with DataProducing {

    override def satisfyGoal(goal: Goal, goalPeriod: Witness) {
        if (!goal.isInstanceOf[HiveGoal])
            throw new IllegalArgumentException("Only HiveGoals are supported")
    }

}

object HiveSatisfier extends HiveSatisfier( MetaStore)
