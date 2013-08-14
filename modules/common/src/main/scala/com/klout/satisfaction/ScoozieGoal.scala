package com.klout.satisfaction

import com.klout.scoozie._
import com.klout.scoozie.dsl._
import com.klout.scoozie.runner._

object ScoozieGoal {

    def apply(workflow: Workflow,
              overrides: Option[ParamOverrides],
              outputs: Set[DataOutput]): Goal = {

        val name = "ScoozieGoal :: " + workflow.name
        val evidence: Set[Evidence] = outputs.toSet[Evidence]
        val satisfier = new ScoozieSatisfier(workflow)
        val variables = (for (data <- outputs) yield {
            data.variables
        }).flatten

        new Goal(name,
            Some(satisfier),
            variables,
            null, /// overrides
            null, /// dependencies
            evidence
        )
    }

    def apply(workflow: Workflow,
              outputs: Set[DataOutput]): Goal = {
        apply(workflow, null, outputs)
    }

}