package satisfaction.notifier

import satisfaction.notifier.Notifier

/**
 *  Notified is a trait which tracks can inherit 
 *     if they want to be notified of a 
 *    Goal'es execution result
 *      
 */
trait Notified {

    def notifier : Notifier
    
    def notifyOnFailure : Boolean = true;
    
    def notifyOnSuccess : Boolean = false;

}