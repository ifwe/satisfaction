package com.klout
package satisfaction

/**
 *  DataOutput represents an abstract location,
 *    which can produce multiple DataInstances
 */
trait DataOutput extends Evidence {

    def variables: Set[Variable[_]]
    
    def exists(witness: Witness): Boolean
    
    def getDataInstance(witness: Witness): Option[DataInstance]

}


object DataDependency {
  
   /**
    *   Create a DataDependency Goal,   
    *     given a DataOutput
    */
   def apply( depData : DataOutput )  : Goal = {
     Goal(s"Data ${depData.toString} ", None, depData.variables)
   }
}

