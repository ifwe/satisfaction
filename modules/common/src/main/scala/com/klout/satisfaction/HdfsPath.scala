package com.klout.satisfaction

import org.joda.time.DateTime

import hive.ms.Hdfs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus

case class HdfsPath(val path: Path) extends DataInstance {

    lazy val status: FileStatus = Hdfs.fs.getFileStatus(path)

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
        Hdfs.exists(path)
    }

}
