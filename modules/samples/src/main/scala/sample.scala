package com.klout.satisfaction
package samples

import org.joda.time._

object `package` {

    object Date extends Param[LocalDate]("dateString")
    object NetworkAbbr extends Param[String]("networkAbbr")

    val Id: Witness => Witness = x => x

    val WithNetwork: String => Witness => Witness = network => witness => witness update NetworkAbbr -> network

    val WaitForKsUidMapping = Goal(
        name = "wait_for_ksuid_mapping",
        satisfier = None,
        variables = Set(Date),
        overrides = None,
        dependencies = Set.empty,
        evidence = Set(
            VariablePath("/data/prod/jobs/ksuid_mapping_output/${dateString}")
        )
    )

    val FeatureGeneration = Goal(
        name = "feature_generation",
        satisfier = None, // will be Scoozie once implemented
        variables = Set(Date, NetworkAbbr),
        overrides = ParamOverrides(NetworkAbbr) set (
            "tw" -> Set.empty, // ...
            "fb" -> Set.empty // ...
        ),
        dependencies = Set(Id -> WaitForKsUidMapping),
        evidence = Set(
            VariablePath("/data/hive/maxwell/hb_feature_import/${dateString}/${networkAbbr}")
        )
    )

    val ScoreCalculation = Goal(
        name = "score_calculation",
        satisfier = None, // will be Scoozie once implemented
        variables = Set(Date),
        overrides = None,
        dependencies = Set(
            WithNetwork("tw") -> FeatureGeneration,
            WithNetwork("fb") -> FeatureGeneration
        ),
        evidence = Set(
            VariablePath("/data/hive/maxwell/maxwell_score/${dateString}")
        )
    )

    val MaxwellPipeline = Project(
        name = "maxwell",
        topLevelGoals = Set(ScoreCalculation),
        projectParameters = ParamMap(
            Date -> new LocalDate("2013-01-01"),
            NetworkAbbr -> "tw"
        ),
        witnessGenerator = WitnessGenerator {
            case thing @ Some(_) => thing
            case None            => None
        }
    )
}

object MaxwellProject extends ProjectProvider(MaxwellPipeline)