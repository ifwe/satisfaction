package com.klout
package satisfaction.fs

import org.joda.time.DateTime

trait FileStatus {
  
    def getSize : Long
    def isDirectory : Boolean
    def isFile : Boolean
    
    def getPath : Path
    
    def lastAccessed : DateTime
    def created : DateTime

}