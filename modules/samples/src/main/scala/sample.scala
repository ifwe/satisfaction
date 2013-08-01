package com.klout.satisfaction
package samples

import common._
import dsl._
import scoozie._

import org.joda.time._

object `package` {

    object scoozies {
        import com.klout.scoozie.{ dsl => ScoozieDsl }
        val ScoreCalculationWf: ScoozieDsl.Workflow = null
        val FeatureGenerationWf: ScoozieDsl.Workflow = null
        val WaitForKsUidWf: ScoozieDsl.Workflow = null
        val MomentGenerationWf: ScoozieDsl.Workflow = null
    }

    import scoozies._

    object MaxwellParameters {
        val DateString = "dateString"
        val Network = "network"
    }

    val KsUidInHdfs: ExternalGoal = ExternalGoal(
        dependsOn = Set(HdfsPath("${nameNode}/data/prod/jobs/ksuid_assigned/${dateString}")),
        variableParams = Set(MaxwellParameters.DateString)
    )

    val WaitForKsUid = InternalGoal(
        name = "wait_for_ksuid_mapping",
        satisfier = ScoozieJob(WaitForKsUidWf),
        variableParams = Set(MaxwellParameters.DateString),
        externalDependsOn = Set(KsUidInHdfs),
        dependsOn = null,
        outputs = Set(HiveTable("ksuid_mapping"), HdfsPath("foo:"))
    )

    def RawContent(network: String): ExternalGoal = ExternalGoal(
        dependsOn = Set(HdfsPath("${nameNode}/data/prod/jobs/rawContent/${network}/${dateString}")),
        variableParams = Set(MaxwellParameters.DateString, MaxwellParameters.Network)
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
            externalDependsOn = Set(RawContent(network)),
            outputs = Set(HiveTable("hb_feature_import"))
        )
        network -> goal
    }) toMap

    val FeatureGenerations: Set[InternalGoal] = FeatureGenerationMap.values.toSet

    val MomentGenerations = for ((network, featureGeneration) <- FeatureGenerationMap) yield InternalGoal(
        name = s"moment_generation_${network}",
        satisfier = ScoozieJob(MomentGenerationWf),
        variableParams = Set(MaxwellParameters.DateString),
        constantParams = NetworkAbbr(network),
        dependsOn = Set(featureGeneration),
        externalDependsOn = null,
        outputs = Set(HiveTable(s"moments_materialized_${network}")))

    val ScoreCalculation = InternalGoal(
        name = "score_calculation",
        satisfier = ScoozieJob(ScoreCalculationWf),
        variableParams = Set(MaxwellParameters.DateString),
        dependsOn = FeatureGenerations,
        externalDependsOn = null,
        outputs = Set(HiveTable("maxwell_score"), HiveTable("network_score")))

    val MaxwellPipeline = Project(
        name = "maxwell",
        goalContextGenerator = DailyGoalContextGenerator(hour = 0, minute = 0),
        goals = Set(WaitForKsUid, ScoreCalculation) ++ FeatureGenerations ++ MomentGenerations)

    val SmallPipeline = Project(
        name = "small",
        goalContextGenerator = DailyGoalContextGenerator(hour = 2, minute = 3),
        goals = Set(WaitForKsUid))

}

object MaxwellProject extends ProjectProvider(MaxwellPipeline)

object SmallProject extends ProjectProvider(SmallPipeline)