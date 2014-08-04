package satisfaction
package hadoop
package hdfs

import org.joda.time.DateTime

import fs._

case class HdfsPath(val path: Path)(implicit val hdfs : FileSystem ) extends DataInstance {

    lazy val status: FileStatus = hdfs.getStatus(path)


    def size: Long = {
      status.size
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
