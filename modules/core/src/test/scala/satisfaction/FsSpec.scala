package satisfaction
package fs

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import PathImplicits._

@RunWith(classOf[JUnitRunner])
class FsSpec extends Specification {

    
  "FsSpec" should { 
     
     "handle slash operator" in {
       val p : Path = new Path( "hdfs://jobs-dev-hnn")
       
       val slashP = p / "data" 
       
       println(s" Slashed path = $slashP")
       
     }
     
     "handle multiple slash operator" in {
       val p : Path = new Path( "hdfs://jobs-dev-hnn/")
       
       val slashP = p / "/data" 
       
       println(s" Slashed path = $slashP")
       
       slashP.toString  must_== "hdfs://jobs-dev-hnn/data"
       
     }
     
     "Get ParentPath" in {
       val p = new Path("/my/really/long/path/name")
       val parent = p.parent
       
       println(s" parent = $parent")
       
       parent.toString must_== "/my/really/long/path" 
     }
     
     
     "convert from URI's " in {
       
       val hdfsURI = new java.net.URI("hdfs://jobs-dev-hnn")
       
       val p : Path = hdfsURI
       
       println(s" PAth is $p")
       
     }
     
     "List local directory" in {
       val fs : FileSystem = LocalFileSystem
       val p = new Path( System.getProperty("user.dir") + "/modules/core/src/test/resources/localFS")
       val allFiles = fs.listFiles( p / "dir1")
       allFiles.foreach( println( _ ))
       
     }
  }
}