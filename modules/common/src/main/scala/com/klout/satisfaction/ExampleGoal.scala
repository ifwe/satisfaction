package com.klout.satisfaction

import com.klout.satisfaction.ExternalDataGoal
import com.klout.satisfaction.HiveTable


class FactContent extends HiveGoal(
        name = "Fact Content",
        hive_file = "fact_content.hql",
        variables = List("dt","network_abbr").toSet,
        ///dependencies ={ case w: Witness 
              ///=> Set( (new  ExternalDataGoal( new HiveTable("bi_maxwell", "ksuid_mapping")), w)) } ,
        dependencies ={ case w: Witness => 
               Set( (new  ExternalDataGoal( new HiveTable("bi_maxwell", "ksuid_mapping")), w)) } ,
        output = Set( new HiveTable("bi_maxwell", "actor_action" ) )
     
) {
}
object FactContent extends FactContent

object KloutFactContent 
   extends FactContent {
        val hive_file = "klout_fact_content.hql"
}

object ExampleGoal  extends Goal(
       name = "MyExample",
       satisfier = null,
       variables = Set("dt","network_abbr"),
       dependencies = {
          case w : Witness if w.variableValues.get("network_abbr").equals("kl") => Set( ( KloutFactContent , w ))
          case w : Witness  =>
            Set( ( FactContent, w ))
       } ,
       outputs = Set( new HiveTable("bi_maxwell", "my_table"))
    )
  {
}