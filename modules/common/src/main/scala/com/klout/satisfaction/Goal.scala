package com.klout.satisfaction 

  


case class Goal(
    name: String,
    satisfier: Satisfier,
    variables: Set[String] = Set.empty,
    dependencies: PartialFunction[Witness,Set[Tuple2[Goal,Witness]]],
    outputs: Set[DataOutput]) {

}


