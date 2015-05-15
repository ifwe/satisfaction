package satisfaction.hadoop

import satisfaction.Satisfier
import satisfaction.Evidence
import satisfaction.hadoop.hdfs.VariablePath
import satisfaction.ExecutionResult
import satisfaction.fs.FileSystem
import satisfaction.Witness

/**
 *  Asssert that a Path actually exists on a filesystem
 *  
 */
class PathExists( varPath :  VariablePath )  extends Satisfier {

    def name : String = s"PathExists(${varPath.pathTemplate})"

    /**
     *  Make sure the Goal is satisfied for the specified Witness
     *  
     *  @returns Result of execution
     */
    def satisfy(witness: Witness): ExecutionResult = robustly {
      if(  !varPath.exists(witness)) {
        varPath.hdfs.mkdirs( varPath.getPathForWitness( witness).get)
      }
      true
    }
    
    /**
     *  If possible, abort the job
     */
    /// Not supported
    def abort() : ExecutionResult = robustly { true }
    

}