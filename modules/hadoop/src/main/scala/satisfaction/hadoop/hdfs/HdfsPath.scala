package com.klout
package satisfaction
package hadoop
package hdfs

import org.joda.time.DateTime

import fs._

case class HdfsPath(val path: Path) extends DataInstance {

    lazy val status: FileStatus = Hdfs.getStatus(path)
    

    lazy implicit val hdfs: Hdfs = Hdfs

    def size: Long = {
        Hdfs.getSpaceUsed(path)
    }

    def created: DateTime = {
        status.created
    }

    def lastAccessed: DateTime = {
      status.lastAccessed
    }

    def exists: Boolean = {
        if (hdfs.exists(path)) {
            if (hdfs.exists( new Path(path.toString + "/_SUCCESS"))) {
                return true
            } else {
                return false
            }
        } else {
            return false
        }
    }

    override def toString: String = {
        path.toUri.toString
    }
}
