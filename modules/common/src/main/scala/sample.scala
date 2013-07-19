package com.klout.satisfaction
package common
package sample

import dsl._
import scoozie._

import org.joda.time._

object `package` {

    object scoozies {
        import com.klout.scoozie.{ dsl => ScoozieDsl }
        val ScoreCalculationWf: ScoozieDsl.Workflow = ???
        val FeatureGenerationWf: ScoozieDsl.Workflow = ???
        val WaitForKsUidWf: ScoozieDsl.Workflow = ???
        val MomentGenerationWf: ScoozieDsl.Workflow = ???
    }

    import scoozies._

    object MaxwellParameters {
        val DateString = "dateString"
    }

    val KsUidInHdfs: ExternalGoal = ExternalGoal(
        dependsOn = Set(HdfsPath("${nameNode}/data/prod/jobs/ksuid_assigned/${dateString}")),
        variableParams = Set(MaxwellParameters.DateString)
    )

    val WaitForKsUid = InternalGoal(
        name = "wait_for_ksuid_mapping",
        satisfier = ScoozieJob(WaitForKsUidWf),
        variableParams = Set(MaxwellParameters.DateString),
        dependsOn = Set(KsUidInHdfs),
        outputs = Set(HiveTable("ksuid_mapping"), HdfsPath("foo:"))
    )

    val networks: Set[String] = Set("tw", "fb", "fp")
    def NetworkAbbr(network: String): Map[String, String] = Map("networkAbbr" -> network)

    val FeatureGenerationMap = (for (network <- networks) yield {
        val goal = InternalGoal(
            name = s"feature_generation_${network}",
            satisfier = ScoozieJob(FeatureGenerationWf),
            variableParams = Set(MaxwellParameters.DateString),
            constantParams = NetworkAbbr(network),
            dependsOn = Set(WaitForKsUid),
            outputs = Set(HiveTable("hb_feature_import"))
        )
        network -> goal
    }) toMap

    val FeatureGenerations: Set[Goal] = FeatureGenerationMap.values.toSet

    val MomentGenerations = for ((network, featureGeneration) <- FeatureGenerationMap) yield InternalGoal(
        name = s"moment_generation_${network}",
        satisfier = ScoozieJob(MomentGenerationWf),
        variableParams = Set(MaxwellParameters.DateString),
        constantParams = NetworkAbbr(network),
        dependsOn = Set(featureGeneration),
        outputs = Set(HiveTable(s"moments_materialized_${network}")))

    val ScoreCalculation = InternalGoal(
        name = "score_calculation",
        satisfier = ScoozieJob(ScoreCalculationWf),
        variableParams = Set(MaxwellParameters.DateString),
        dependsOn = FeatureGenerations,
        outputs = Set(HiveTable("maxwell_score"), HiveTable("network_score")))

    val MaxwellPipeline = Project(
        name = "maxwell",
        goalPeriodGenerator = DailyGoalContextGenerator(hour = 0, minute = 0),
        goals = Set(WaitForKsUid, ScoreCalculation) ++ FeatureGenerations ++ MomentGenerations)

}