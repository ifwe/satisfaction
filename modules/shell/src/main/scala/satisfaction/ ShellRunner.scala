package satisfaction
package shell

import com.klout.satisfaction.Evidence
import com.klout.satisfaction.Witness
import com.klout.satisfaction.Track
import java.io.File
import scala.sys.process._
import java.io.FileOutputStream
import com.klout.satisfaction.fs.LocalFileSystem
import com.klout.satisfaction.fs.Path

case class ShellRunner(val shellResource : String)( implicit val track : Track) {  
  
   val runDirectory : Path =  new Path( "./SatisfactionShell/")
   val lfs = new LocalFileSystem()
        
    var p : Process = null
        
    def run(  w : Witness ) : Boolean = {
       val shellPrefix = copyToLocal (shellResource, track)
       val args = w.assignments.map ( vass => {
          val vname = vass.variable.name
          val vval = vass.value.toString
           s"-D${vname}=${vval}"
        }).toSeq
        
        val shellCmd = Seq(shellPrefix.toString) ++ args

        println(s"Executing command '${shellCmd.mkString(" ")}'")
        
        val pio = new ProcessIO(_ => (),
                        stdout => scala.io.Source.fromInputStream(stdout)
                          .getLines.foreach(ln =>{println(s"##OUT##${shellResource}##${ln}")}),
                        stderr => scala.io.Source.fromInputStream(stderr)
                          .getLines.foreach( ln => {println(s"##ERR##${shellResource}##${ln}")}))
        val pb = Process( shellCmd)
        p = pb.run(pio) 
        
        p.exitValue  == 0
     }

     
     def abort()  : Boolean = {
       if( p != null)  {
         p.destroy
       }
       true
     }
     
     
     def copyToLocal( shellResource : String, track : Track ) : Path = {
       val localShell = runDirectory / shellResource
       track.hdfs.copyToFileSystem(lfs, track.resourcePath / shellResource , localShell)
       lfs.setExecutable( localShell)

       localShell
     }
     

}