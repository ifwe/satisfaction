package models

import com.klout.satisfaction._
import org.joda.time.DateTime

object ProjectUtil {

    /// place utility code here ..
    def blah = {

    }
}


object HtmlUtil {
    
    def formatDate( dt : DateTime ) : String = {
       dt match {
         case null => "N/A"
         case d => 
           d.toString
       } 
    }
    
    def formatWitness( wit : Witness ) : String = {
       wit.toString 
    }
    
      
    def parseWitness( varString : String ) : Witness = {
      val vaSeq : Seq[VariableAssignment[String]] = varString.split(";").map( _.split("=") ).map( kvArr  => 
           { VariableAssignment[String](Variable( kvArr(0)), kvArr(1) ) } )
      
       Witness( vaSeq:_*)
    }
    
    /** 
     *   Convert a witness to a String which can be passed as a string in an URL
     */
    def witnessPath( witness : Witness ) : String = {
      witness.substitution.assignments.map( ass => {
           s"${ass.variable.name}=${ass.value}"
      } ).mkString(";")
    }
}