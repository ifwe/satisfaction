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
    object dateParam extends Param[String]("dt")
    object networkParam extends Param[String]("network_abbr")
    object serviceIDParam extends Param[Int]("service_id")

    val calcScore = ScoozieGoal(
        workflow = ScoreCalculation.CalcScore,
        Set(HiveTable("bi_maxwell", "maxwell_score"),
            HiveTable("bi_maxwell", "network_score"))
    )
    val featureNetworks: Set[Network] =
        Set(Networks.Klout, Networks.Facebook, Networks.Twitter,
            Networks.LinkedIn, Networks.Foursquare, Networks.FacebookPages)

    def qualifyWitness(networkAbbr: String): (Witness => Witness) = {
        w: Witness =>
            val newParam = w.params.update(networkParam, networkAbbr)
            new Witness(newParam)
    }
    for (network <- featureNetworks) {
        calcScore.addWitnessRule(qualifyWitness(network.networkAbbr), featureGenGoal(network))
    }

    def featureGenGoal(networkName: Network): Goal = {
        val featureGen = ScoozieGoal(workflow = FeatureGeneration.Finalize,
            Set(HiveTable("bi_maxwell", "hb_feature_import")))
        ///featureGen.addDependency(factContentGoal(networkName))
        featureGen
    }

    def factContentGoal(networkName: Network): Goal = {
        networkName.networkAbbr match {
            case "klout" =>
                val kloutAA: Goal = HiveGoalFactory.forTableFromFile(
                    "Klout Fact Content",
                    HiveTable("bi_maxwell", "actor_action"),
                    "fact_content_kl.hql")
                kloutAA.addWitnessRule({ w: Witness =>
                    val newParam = w.params.update(serviceIDParam, networkName.featureGroup);
                    new Witness(newParam)
                },
                    WaitForKSUIDMappingGoal)
            case _ =>
                val actorAction: Goal = HiveGoalFactory.forTableFromFile(
                    networkName.networkFull + " Fact Content",
                    HiveTable("bi_maxwell", "actor_action"),
                    "fact_content.hql")
                actorAction.addDependency(WaitForKSUIDMappingGoal)
        }
    }

    val WaitForKSUIDMappingGoal: Goal = ScoozieGoal(
        workflow = WaitForKsUidMapping.Flow,
        Set(HiveTable("bi_maxwell", "ksuid_mapping")))

    val Project = new Project("Maxwell Score",
        Set(calcScore),
        ParamMap((dateParam -> "20130815")), //// sic
        null)

}