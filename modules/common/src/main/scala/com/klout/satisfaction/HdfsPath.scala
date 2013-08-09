package com.klout.satisfaction

import org.joda.time.DateTime

import hive.ms.Hdfs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus

class HdfsPath(path: Path) extends DataInstance {

    val status: FileStatus = Hdfs.fs.getFileStatus(path)

    def getSize: Long = {
        Hdfs.getSpaceUsed(path)
    }

    def created: DateTime = {
        new DateTime(status.getModificationTime)
    }
    def lastAccessed: DateTime = {
        new DateTime(status.getModificationTime)
    }

}