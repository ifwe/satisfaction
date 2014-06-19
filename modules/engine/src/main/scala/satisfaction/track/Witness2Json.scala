package com.klout
package satisfaction
package track



import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.klout.satisfaction.Witness

/**
 *  Provide simple bindings to JSON 
 *    using json4s
 *  
 *  Lot's of implicits going on here 
 *    Move to a different package
 *   (and add more bindings)
 *   if we implement a SOA type architecture
 */
object  Witness2Json {

    implicit def Witness2JValue( witness : Witness) : JValue = {
      witness.raw
    }
     
    implicit def JValue2Witness( jval : JValue  ) : Witness = {
       jval match {
         case jmap : JObject => {
           val rawMap : Map[String,String] = jmap.values.asInstanceOf[Map[String,String]]
           Witness(rawMap)
         }
         case _ => {
           throw new RuntimeException(s"Unable to interpret ${render(jval)} as a Witness map")
         }
       }
    }

    
    def parseWitness( jsonString : String ) : Witness = {
        parse(jsonString) 
    }
    
    def renderWitness( witness : Witness) : String = {
       compact(render(witness))
    }
}