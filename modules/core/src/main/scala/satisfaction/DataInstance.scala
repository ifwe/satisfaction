package satisfaction

import org.joda.time._

/**
 *  Trait for a class containing   
 *    a certain amount of actual data 
 */
trait DataInstance {
    def size: Long
    def created: DateTime
    def lastAccessed: DateTime

}

/**
 *  After it has been completely created, 
 *    it can be marked as complete
 *    so that we know it is not halfway done .. 
 *    
 *    For example, most Hadoop jobs place a "_SUCCESS" 
 *      file in the path, to signify that the job 
 *       ran, and completed without terminal errors.
 */
trait Markable {

    /**
     *  Mark that the producer of this
     *   DataInstance fully completed .
     */
    def markCompleted : Unit
    
    
    /**
     * Mark that this DataInstance has 
     *   not been fully generated,
     *   and should be redone.
     */
    def markIncomplete : Unit
    
    /**
     *  Check that the Data instance has been Marked completed,
     *    according to the test of the markable.
     */
    def isMarkedCompleted : Boolean
  
}

trait Sane {

    def checkSanity  : Boolean
}

