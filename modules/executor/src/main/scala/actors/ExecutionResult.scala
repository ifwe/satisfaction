package com.klout.satisfaction
package executors

package actors

import org.joda.time.DateTime

/**
 *  Object which tells us about the execution run,
 *   ie. was it successful, what where any sub tasks,
 *   how long did it take, how much data was produced , etc ..oo
 */
case class ExecutionResullt(
    startTime: DateTime,
    endTime: DateTime,
    metSLA: Boolean) {

}