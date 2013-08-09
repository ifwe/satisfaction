package com.klout.satisfaction

import org.joda.time._

abstract class DataInstance {

    def getSize: Long
    def created: DateTime
    def lastAccessed: DateTime

}