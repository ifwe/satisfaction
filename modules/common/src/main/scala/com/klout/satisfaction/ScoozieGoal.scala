package com.klout.satisfaction

import com.klout.scoozie._
import com.klout.scoozie.dsl._
import com.klout.scoozie.runner._

class ScoozieGoal(name: String,
                  variables: Set[String],
                  dependencies: Set[Goal],
                  outputs: Set[DataOutput])
    extends Goal(name, ScoozieSatisfier, variables, dependencies, outputs) {

    /// XXX TODO get OozieConfig from input parameters ...
    val oozieConfig: OozieConfig = ???
    val appPath: String = ???

}