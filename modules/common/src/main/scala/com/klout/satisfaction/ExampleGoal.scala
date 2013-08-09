package com.klout.satisfaction

import com.klout.satisfaction.ExternalGoal
import com.klout.satisfaction.HiveTable


object FactContent extends HiveGoal(
        name = "Fact Content",
        variables = List("dt","network_abbr").toSet,
        dependencies ={ List(  ExternalGoal( HiveTable("bi_maxwell", "ksuid_mapping")))} ,
        List( HiveTable("bi_maxwell", "actor_action" ))
     
) {
  
}

object ExampleGoal  extends HiveGoal(
       "MyExample",
       List("dt","network_abbr"),
       { case _.variableMap.get("network_abbr").equal("kl") =>
             Set( ( KloutFactContent , _ ))
       case default  
            Set( ( FactContent, _ ))
       } 
    )
  {

}