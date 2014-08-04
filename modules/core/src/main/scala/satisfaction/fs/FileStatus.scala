package satisfaction.fs

import org.joda.time.DateTime

trait FileStatus {
  
    def size : Long
    def isDirectory : Boolean
    def isFile : Boolean
    
    def path : Path
    
    def lastAccessed : DateTime
    def created : DateTime

}