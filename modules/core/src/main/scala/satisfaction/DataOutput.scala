package com.klout
package satisfaction

/**
 *  DataOutput represents an abstract location,
 *    which can produce multiple DataInstances
 */
trait DataOutput extends Evidence {

    def variables: List[Variable[_]]
    
    def exists(witness: Witness): Boolean
    
    def getDataInstance(witness: Witness): Option[DataInstance]

}


object DataDependency {
  
   /**
    *   Create a DataDependency Goal,   
    *     given a DataOutput
    */
   def apply( depData : DataOutput )( implicit track : Track)  : Goal = {
     Goal(name= s"Data Dependency ${depData.toString} ", 
          satisfier=None,
          variables=depData.variables,
          dependencies=Set.empty,
          evidence = Set(depData))
   }
}

