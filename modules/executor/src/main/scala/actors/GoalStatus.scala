package com.klout.satisfaction
package executor
package actors

import org.joda.time.DateTime

/**
 *  For a running job, what is the current Status of the
 *    Goal
 *
 *    Includes information about all the dependent children as
 *    well
 */

object GoalState extends Enumeration {
    type State = Value
    val Unstarted, AlreadySatisfied, WaitingOnDependencies, Running, DependencyFailed, Failed, Success, Aborted = Value

}

case class GoalStatus(goal: Goal, witness: Witness) {

    var state: GoalState.Value = GoalState.Unstarted

    var dependencyStatus = scala.collection.mutable.Map[String, GoalStatus]()

    var timeStarted: DateTime = null
    var timeFinished: DateTime = null

    var errorMessage: String = null

    def addChildStatus(child: GoalStatus): GoalStatus = {
        val predName = child.goal.getPredicateString(child.witness)
        dependencyStatus.put(predName, child)
        this
    }
    
    var execResult : ExecutionResult = null
    
    
    /**
     *  Return true if the actor's state
     *   can no longer change
     */
    def isTerminal : Boolean = {
       state == GoalState.Success ||
         state == GoalState.Failed ||
         state == GoalState.Aborted || 
         state == GoalState.DependencyFailed
    }

}