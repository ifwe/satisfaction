package com.klout
package satisfaction
package fs

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import PathImplicits._

@RunWith(classOf[JUnitRunner])
class FsSpec extends Specification {

    
     
     "handle slash operator" in {
       val p : Path = new Path( "hdfs://jobs-dev-hnn")
       
       val slashP = p / "data" 
       
       println(s" Slashed path = $slashP")
       
     }
     
     "convert from URI's " should {
       
       val hdfsURI = new java.net.URI("hdfs://jobs-dev-hnn")
       
       val p : Path = hdfsURI
       
       println(s" PAth is $p")
       
     }
     
     "List local directory" should {
       val fs : FileSystem = new LocalFileSystem(System.getProperty("user.dir") + "/modules/core/src/test/resources/localFS")
       val allFiles = fs.listFiles( new Path("dir1"))
       allFiles.foreach( println( _ ))
       
     }
}