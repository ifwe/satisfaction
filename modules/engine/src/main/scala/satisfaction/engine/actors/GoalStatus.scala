package com.klout
package satisfaction
package engine
package actors

import org.joda.time.DateTime

/**
 *  For a running job, what is the current Status of the
 *    Goal
 *
 *    Includes information about all the dependent children as
 *    well
 *    XXX make immutable 
 */

object GoalState extends Enumeration {
    type State = Value
    val Unstarted, AlreadySatisfied, WaitingOnDependencies, Running, DependencyFailed, Failed, Success, Aborted = Value

}

case class GoalStatus(track : TrackDescriptor, goalName: String, witness: Witness) {

    // XXX Make immutable ... ????
    var state: GoalState.Value = GoalState.Unstarted

    var dependencyStatus = scala.collection.mutable.Map[String, GoalStatus]()

    val timeStarted: DateTime = DateTime.now
    var timeFinished: DateTime = null

    var errorMessage: String = null

    def addChildStatus(child: GoalStatus): GoalStatus = {
        val predName = Goal.getPredicateString(child.goalName, child.witness)
        dependencyStatus.put(predName, child)
        this
    }
    
    def numReceivedStatuses = {
       dependencyStatus.size
    }
    
    def canProceed = {
      dependencyStatus.values.forall( stat => {GoalStatus.canProceed( stat.state )  } )
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
    
    /**
     *  Return true if the actor's state can change
     */
      def canChange : Boolean = {
        state == GoalState.WaitingOnDependencies || 
        state == GoalState.Running
      }
}

object GoalStatus {
    def canProceed( gs : GoalState.Value  )  : Boolean = {
       gs == GoalState.Success || gs == GoalState.AlreadySatisfied 
    }
}