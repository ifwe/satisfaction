package com.klout
package satisfaction
package hadoop
package hdfs

import org.joda.time.DateTime

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus

case class HdfsPath(val path: Path) extends DataInstance {

    lazy val status: FileStatus = Hdfs.fs.getFileStatus(path)

    lazy implicit val hdfs: Hdfs = Hdfs

    def size: Long = {
        Hdfs.getSpaceUsed(path)
    }

    def created: DateTime = {
        new DateTime(status.getModificationTime)
    }

    def lastAccessed: DateTime = {
        new DateTime(status.getModificationTime)
    }

    def exists: Boolean = {
        if (hdfs.exists(path)) {
            if (hdfs.exists(new Path(path.toUri() + "/_SUCCESS"))) {
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
