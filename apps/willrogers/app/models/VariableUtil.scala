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
}