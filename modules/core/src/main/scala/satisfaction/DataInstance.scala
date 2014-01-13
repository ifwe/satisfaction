package com.klout
package satisfaction

import org.joda.time._

trait DataInstance {
    def size: Long
    def created: DateTime
    def lastAccessed: DateTime
    def exists: Boolean
}