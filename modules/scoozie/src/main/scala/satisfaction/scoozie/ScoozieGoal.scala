package com.klout
package satisfaction
package hadoop
package scoozie

import com.klout.scoozie._
import com.klout.scoozie.dsl._
import com.klout.scoozie.runner._

object ScoozieGoal {

    def apply(name: String,
              workflow: Workflow,
              overrides: Option[Substitution] = None,
              outputs: Set[DataOutput])
    	(implicit  track : Track): Goal = {

        val evidence: Set[Evidence] = outputs.toSet[Evidence]
        val satisfier = new ScoozieSatisfier(workflow)
        val variables = (for (data <- outputs) yield {
            data.variables
        }).flatten

        new Goal(name,
            Some(satisfier),
            variables,
            Set.empty, /// dependencies
            evidence
        )
    }

    def apply(workflow: Workflow,
              outputs: Set[DataOutput])
      (implicit track : Track): Goal = {
        apply( workflow.name, workflow, null, outputs)

    }

}