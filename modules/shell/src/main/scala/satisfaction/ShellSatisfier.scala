package satisfaction
package shell

import com.klout.satisfaction.Satisfier
import com.klout.satisfaction.ExecutionResult
import com.klout.satisfaction.Witness
import com.klout.satisfaction.Track

class ShellSatisfier(val shellCommand : String )
     ( implicit val track : Track ) extends Satisfier {
  
    override def name = s"ShellExec: $shellCommand "
    
    val shellRunner  : ShellRunner = new ShellRunner( shellCommand)
    
    def satisfy(witness: Witness): ExecutionResult = {
      robustly {
        shellRunner.run(witness)
      }
    }
    
    /**
     *  If possible, abort the job
     */
    def abort() : ExecutionResult = {
        robustly   {
            shellRunner.abort
       }
    }

}