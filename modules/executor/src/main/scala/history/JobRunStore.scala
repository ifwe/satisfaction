package com.klout.satisfaction
package executor
package history

import actors.GoalStatus

/**
 * Abstract away the persistence for job runs...
 *   Link to the metastore ...
 *    
 *   XXX  Should we use hraven here ??
 *     or be agnostic ?/ 
 */
trait JobHistoryStore {

      def recordGoalStatus(  goalStatus : GoalStatus )
}