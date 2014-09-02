package satisfaction

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
    val Unstarted, 
    	AlreadySatisfied, 
    	WaitingOnDependencies,
    	Running, 
    	DependencyFailed, 
    	Failed, 
    	Success, 
    	Aborted,
    	Queued, 
    	Forced = Value
}

case class GoalStatus(track : TrackDescriptor, goalName: String, witness: Witness) {

    private var _state: GoalState.Value = GoalState.Unstarted
    
    def state : GoalState.Value = { _state }

    private val _dependencyStatus = scala.collection.mutable.Map[String, GoalStatus]()
    def dependencyStatus : collection.immutable.Map[String,GoalStatus] = { _dependencyStatus.toMap } 
    
    private var _progressCounter : Option[ProgressCounter] = None
    def progressCounter : Option[ProgressCounter] =  _progressCounter
    def setProgressCounter( pc : ProgressCounter) = { _progressCounter = Some(pc) }

    val timeStarted: DateTime = DateTime.now

    def timeFinished =  _timeFinished 
    private var _timeFinished: DateTime = null

    private var _execResult : ExecutionResult = null
    def execResult : ExecutionResult = _execResult

    
    def errorMessage : String = _errorMessage
    private var _errorMessage: String = null

    def addChildStatus(child: GoalStatus): GoalStatus = {
        val predName = Goal.getPredicateString(child.goalName, child.witness)
        _dependencyStatus.put(predName, child)
        this
    }
    
    def numReceivedStatuses = {
       _dependencyStatus.size
    }
    
    def canProceed = {
      _dependencyStatus.values.forall( stat => {GoalStatus.canProceed( stat.state )  } )
    }
    
    def markTerminal( state : GoalState.Value, finish : DateTime = DateTime.now) = {
       _state = state 
       _timeFinished = finish
    }
    
    def markExecution( execResult : ExecutionResult ) = {
      _execResult = execResult
      _timeFinished = DateTime.now
      if(execResult.isSuccess ) {
        _state = GoalState.Success
      } else {
        if(_state != GoalState.Aborted)
           _state = GoalState.Failed
           
        if( execResult.errorMessage != null) 
          _errorMessage = execResult.errorMessage
      }
    }
    
    def markUnexpected( error : Throwable) = {
       _state = GoalState.Failed
       _errorMessage = s"Unexpected Exception ${error.getLocalizedMessage()} " 
         /// XXX Catch the stack Trace??? 
    }
    
    def markError( error : String ) = {
       _state = GoalState.Failed
       _errorMessage = error
    }
    
    def transitionState( state: GoalState.Value ) = {
      _state = state
    }

    
   
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