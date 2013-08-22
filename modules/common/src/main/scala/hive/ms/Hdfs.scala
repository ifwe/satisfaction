package hive.ms

import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path

/**
 *
 */
object Hdfs {

    /// Dependency injection ???
    val _fsURI = new URI("hdfs://jobs-dev-hnn:8020")
    ///val _fsURI = new URI("hdfs://jobs-aa-hnn:8020")
    val fs = FileSystem.get(_fsURI, Config.config)

    def exists(path: Path): Boolean = {
        return fs.exists(path)
    }

    def getSpaceUsed(path: Path): Long = {
        var totalLen: Long = 0
        if (Hdfs.fs.exists(path)) {
            val status = Hdfs.fs.getFileStatus(path)
            if (status.isDirectory()) {
                val fsArr = fs.listFiles(status.getPath(), true)
                while (fsArr.hasNext()) {
                    val lfs = fsArr.next
                    totalLen += lfs.getLen()
                }
                return totalLen
            } else {
                status.getLen()
            }
        } else {
            0
        }
    }

    def rounded(dbl: Double): String = {
        (((dbl * 100).toInt) / 100.0).toString
    }
    //  Return a string which translates from a long 
    //   to a string including Mb or Gb or Tb
    def prettyPrintSize(lngSize: Long): String = {
        val kb: Double = lngSize.toDouble / 1024.0
        if (kb < 10) {
            return lngSize + " bytes"
        } else {
            val mb = kb / 1024.0
            if (mb < 1.0) {
                return rounded(kb) + "K"
            } else {
                val gb = mb / 1024.0
                if (gb < 1.0) {
                    return rounded(mb) + " Mb"
                } else {
                    val tb = gb / 1024.0
                    if (tb < 1.0) {
                        return rounded(gb) + " Gb"
                    } else {
                        return rounded(tb) + " Tb"
                    }
                }
            }
        }
    }

}