package satisfaction

/**
 * 
 */
object `package` {
  
   /**
    *  Define a type for function which returns a satisfier 
    */
   type SatisfierFactory = (Witness => Option[Satisfier])
  
}

