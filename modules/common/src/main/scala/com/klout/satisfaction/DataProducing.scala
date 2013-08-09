package com.klout.satisfaction

/**
 *  DataProducing is a trait which states that
 *   I am satisfied if all of my output exists
 *    for the current Witness.
 */
abstract trait DataProducing extends Satisfier {

    override def isSatisfied(goal: Goal, witness: Witness) = {
        goal.outputs.forall(data => data.instanceExists(witness))
    }

}