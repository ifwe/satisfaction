package hive.ms

import scalaxb._
import org.specs2.mutable._
import com.klout.satisfaction.HiveTable
import com.klout.satisfaction.Witness
import com.klout.satisfaction._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import io._

@RunWith(classOf[JUnitRunner])
class HdfsSpec extends Specification {

    "Hdfs" should {
        "create URLS starting with hdfs" in {
          val hdfs = Hdfs
          val hdfsUrl = new java.net.URL("hdfs://jobs-dev-hnn/user/satisfaction/track/usergraph/version_1.1/satisfaction.properties")
         
          val stream = hdfsUrl.openStream
          val props  = Substituter.readProperties( stream)
          
        }

    }

}