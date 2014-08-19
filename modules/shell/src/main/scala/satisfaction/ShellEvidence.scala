package satisfaction
package shell

import com.klout.satisfaction.Evidence
import com.klout.satisfaction.Witness
import com.klout.satisfaction.Track
import java.io.File
import scala.sys.process._

case class ShellEvidence( val shellScript : String )
       (implicit val track : Track ) extends Evidence {
  
  
     val shellRunner = new ShellRunner(shellScript)
  
     override def exists(w: Witness): Boolean = {
          shellRunner.run( w)
     }


}