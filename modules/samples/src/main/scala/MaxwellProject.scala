package com.klout.satisfaction.projects

import com.klout.satisfaction.Project
import com.klout.satisfaction._
import com.klout.scoozie._
import com.klout.klout_scoozie._
import com.klout.klout_scoozie.maxwell._
import workflows._
import com.klout.klout_scoozie.common.Networks
import com.klout.klout_scoozie.common.Network

object MaxwellProject {
    val networkAbbrVar = Variable[String]("network_abbr", classOf[String])
    val featureGroupVar = Variable[String]("feature_group", classOf[String])
    val serviceIdVar = Variable[Int]("service_id", classOf[Int])

    val calcScore = ScoozieGoal(
        workflow = ScoreCalculation.CalcScore,
        Set(HiveTable("bi_maxwell", "maxwell_score"),
            HiveTable("bi_maxwell", "network_score"))
    )
    val featureNetworks: Set[Network] =
        Set(Networks.Klout, Networks.Facebook, Networks.Twitter,
            Networks.LinkedIn, Networks.Foursquare, Networks.FacebookPages)

    def qualifyByNetwork(networkAbbr: String): (Witness => Witness) = {
        w: Witness =>
            w.update(VariableAssignment[String](networkAbbrVar, networkAbbr))
    }

    def qualifyByFeatureGroup(fg: Int): (Witness => Witness) = {
        w: Witness =>
            w.update(VariableAssignment[String](featureGroupVar, fg.toString))
    }

    def getTopLevel: Goal = {
        for (network <- featureNetworks) {
            println(s" Adding dependency on score with features ${network.networkAbbr} ")
            calcScore.addWitnessRule(
                qualifyByFeatureGroup(network.featureGroup),
                featureGenGoal(network))
        }

        return calcScore
    }

    def featureGenGoal(network: Network): Goal = {
        val featureGen = ScoozieGoal(name = s" Feature Generation for ${network.networkFull}",
            workflow = FeatureGeneration.Finalize,
            overrides = None,
            Set(HiveTable("bi_maxwell", "hb_feature_import")))
        featureGen.addWitnessRule(
            Goal.stripVariable(Variable("feature_group")) compose qualifyByNetwork(network.networkAbbr),
            factContentGoal(network))
    }

    def factContentGoal(networkName: Network): Goal = {
        networkName.networkAbbr match {
            case "klout" =>
                val kloutAA: Goal = HiveGoalFactory.forTableFromFile(
                    "Klout Fact Content",
                    HiveTable("bi_maxwell", "actor_action"),
                    "fact_content_kl.hql")
                kloutAA.addWitnessRule(Goal.stripVariable(Variable("network_abbr")),
                    WaitForKSUIDMappingGoal)
            case _ =>
                val actorAction: Goal = HiveGoalFactory.forTableFromFile(
                    networkName.networkFull + " Fact Content",
                    HiveTable("bi_maxwell", "actor_action"),
                    "fact_content.hql")
                actorAction.addWitnessRule(
                    Goal.stripVariable(Variable("network_abbr")),
                    WaitForKSUIDMappingGoal)
        }
    }

    val WaitForKSUIDMappingGoal: Goal = ScoozieGoal(
        workflow = WaitForKsUidMapping.Flow,
        Set(HiveTablePartitionGroup("bi_maxwell", "ksuid_mapping", Variable("dt").asInstanceOf[Variable[Any]])))

    val Project = new Project("Maxwell Score",
        Set(getTopLevel),
        null)

}