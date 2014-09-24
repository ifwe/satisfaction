package models

import satisfaction._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object ProjectUtil {

}


object HtmlUtil {

    val defaultTimeFormat = DateTimeFormat.forPattern("EEE YYYY/MM/dd hh:mm aa")
    val simpleTimeFormat = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")

    def formatDate( dt : DateTime ) : String = {
       dt match {
         case null => "N/A"
         case d => 
           defaultTimeFormat.print(d)
       } 
    }
    def formatDate( dtStr : String) : String = {
      if(dtStr != null) {
         val parseDate = new DateTime(dtStr)
         defaultTimeFormat.print(parseDate)
      } else {
        "N/A" 
      }

    }
    def formatDate( dtOpt : Option[DateTime] ) : String = {
       dtOpt match {
         case null => "N/A"
         case None => "N/A"
         case Some(d) => 
           defaultTimeFormat.print(d)
       } 
    }
    
    def formatTrackHistoryDate(dtOpt: Option[DateTime]) : String = {
      dtOpt match {
        case null => "N/A"
        case None => "N/A"
        case Some(d) =>
        	simpleTimeFormat.print(d)
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
      val witnessPath = witness.assignments.map( ass => {
           s"${ass.variable.name}=${ass.value}"
      } ).mkString(";").replace("/","_sl_")
      
      //println(" HtmlUtil::witnessPath witnessPath is: " + witnessPath)
      witnessPath
      
    }
    
}