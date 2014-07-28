package models

import com.klout.satisfaction._
import org.joda.time.DateTime

object ProjectUtil {

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
    
    def witnessTable( wit : Witness , css : String = "witness") : String = {
       s"<table style=$css>" + 
       wit.assignments.map(  ass => { "<tr><td>" + ass.variable.name + "</td><td>" + ass.value + "</td>" } ).mkString  + "</table>"
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
      witness.assignments.map( ass => {
           s"${ass.variable.name}=${ass.value}"
      } ).mkString(";").replace("/","_sl_")
    }
    
}