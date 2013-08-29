package com.klout.satisfaction

import com.klout.scoozie._
import com.klout.scoozie.dsl._
import com.klout.scoozie.runner._

object ScoozieGoal {

    def apply(name: String,
              workflow: Workflow,
              overrides: Option[Substitution] = None,
              outputs: Set[DataOutput]): Goal = {

        val evidence: Set[Evidence] = outputs.toSet[Evidence]
        val satisfier = new ScoozieSatisfier(workflow)
        val variables = (for (data <- outputs) yield {
            data.variables
        }).flatten

        new Goal(name,
            Some(satisfier),
            variables,
            overrides, /// overrides
            Set.empty, /// dependencies
            evidence
        )
    }

    def apply(workflow: Workflow,
              outputs: Set[DataOutput]): Goal = {
        apply("ScoozieGoal :: " + workflow.name, workflow, null, outputs)

    }

}