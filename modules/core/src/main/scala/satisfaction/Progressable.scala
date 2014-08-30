package satisfaction



        
    
/**
 * Some satisfiers may be able to get insight into how much progress
 *   has been done so for, so that predictions can be made as to
 *     job completion 
 *     
 *  For Hadoop and Hive jobs, this can involve accessing the job client,
 *   or multiple job clients ...
 */
trait ProgressCounter {
  
    /**
     *  Name describing the current task whose 
     *    progress is being tracked
     */
    def taskName : String
  
    /**
     *  List of subtasks which need to be completed 
     *   in order for the task to be done.
     *   
     *  Returns a list of Tuples containing the 
     *   task's name and it's current state.
     */
    def subtasks : List[(String,GoalState.State)]

    /**
     *  Current running subtasks 
     */
    def runningSubTasks : Set[ProgressCounter]

    /**
     *  Rough estimate of total progress of the task so far.
     */
    def progressPercent : Double
    
    /**
     *  measure of units which have completed.
     *  For example Hadoop counters ,
     *   number of query subtasks.
     */
    def progressUnits : MetricsCollection
    
    
    /**
     *  Convenience function for calculating the overall 
     *   progress of a Task from aggregating all the 
     *   subtasks, possibly weighting them by how
     *    long each subtask would usually take as a 
     *    proportion to the whole task
     *   (i.e. The weighting matrix should include 
     *     all the subtasks, and the values should add up to 1.0 )
     *  If a set of weights is not passed in, it is assumed to be
     *   evenly distributed among the subtasks   
     */
    def progressPercentFromSubtasks( weighting : Option[Map[String,Double]]) : Double = {
       subtasks.filter( _._2 == GoalState.Success ).map( t => {
         weighting match {
           case Some(weight) => { weight.get( t._1 ).get }
           case None => { 1/subtasks.length } 
         }} ).sum + runningSubTasks.map( _.progressPercent).sum
    }
  
}

/**
 *  Trait to be applied to a Satisfier
 *   to inform that the task's progress
 *   can be tracked.
 */
trait Progressable {

   def progressCounter : ProgressCounter
   
}