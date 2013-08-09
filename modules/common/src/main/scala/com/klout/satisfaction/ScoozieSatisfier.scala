package com.klout.satisfaction

import com.klout.scoozie._
import com.klout.scoozie.runner._
import com.klout.scoozie.dsl._

class ScoozieSatisfier extends Satisfier with DataProducing {

    override def satisfyGoal(goal: Goal, goalPeriod: Witness) = {
        if (!goal.isInstanceOf[ScoozieGoal])
            throw new IllegalArgumentException("Scoozie Satisfier requires ScoozieGoal")

        val oozieGoal = goal.asInstanceOf[ScoozieGoal]
        //// 
        RunWorkflow.execWorkflow(oozieGoal.appPath, oozieGoal.oozieConfig)

    }
}

object ScoozieSatisfier extends ScoozieSatisfier {

}