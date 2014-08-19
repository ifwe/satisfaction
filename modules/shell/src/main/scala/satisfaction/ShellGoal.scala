package satisfaction
package shell

import com.klout.satisfaction.Variable
import com.klout.satisfaction.Goal
import com.klout.satisfaction.Track
import com.klout.satisfaction.TemporalVariable


/**
 *  Satisfy a goal by running a shell script.
 */
object ShellGoal {
  
    def apply( shellCommand : String, variables : List[Variable[_]] = List.empty)
         ( implicit  track : Track ) : Goal = {
       new Goal(name = s"Shell#${shellCommand}",
    		    satisfier = Some(new ShellSatisfier( shellCommand) ),
    		    variables = if( variables.length != 0 ) { variables } else { List( TemporalVariable.StartTime )} )
    }

}