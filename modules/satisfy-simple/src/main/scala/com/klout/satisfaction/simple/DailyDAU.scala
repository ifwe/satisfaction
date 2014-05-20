package com.klout
package satisfy
package track
package simple

import com.klout.satisfaction._
object SampleGoal {
    
  
    def apply(name: String, numIterations: Int)(implicit track : Track): Goal = {
        val variables: Set[Variable[_]] = Set(Variable("dt"), Variable("network_abbr"), Variable[Int]("service_id", classOf[Int]))
        val sampSatisfier = new SampleSatisfier(numIterations, 1000)
        new Goal(name = name, satisfier = Some(sampSatisfier), variables = variables)

    }

    def apply(name: String, varArg: Set[Variable[_]], numIterations: Int)(implicit track : Track): Goal = {
        val sampSatisfier = new SampleSatisfier(numIterations, 1000)
        new Goal(name = name, satisfier = Some(sampSatisfier), variables = varArg)
    }

}

///object SampleProject extends Track(TrackDescriptor("SampleTrack")) {
class  SampleProject extends Track(TrackDescriptor("SampleTrack")) with Recurring  {
  
     override def frequency  = Recurring.period("1D")  
  
    val dtVar = Variable("dt")
    val networkAbbrVar = Variable[String]("network_abbr", classOf[String])
    val serviceIdVar = Variable[Int]("service_id", classOf[Int])
    
    ///{
       ///println(" Top level goal is " + topLevelGoal)
       ///addTopLevelGoal( topLevelGoal )
    ///}

    val featureNetworks: Set[Network] =
        Set(Networks.Klout, Networks.Facebook, Networks.Twitter,
            Networks.LinkedIn, Networks.Foursquare, Networks.FacebookPages)

    val topLevelGoal: Goal = {
        println(" TopLevel Goal -- Networks = )" + featureNetworks)

        val tlGoal = SampleGoal("Top Level Goal", Set(Variable("dt")), 23)

        val waitForKsuidMapping = SampleGoal("Wait for KSUID Mapping", Set(Variable("dt")), 60)

        for (network <- featureNetworks) {
            println(s" Adding dependency on score with features ${network.networkAbbr} ")
            val subGoal = SampleGoal(network.networkFull + " Features", 20 + (Math.random() * 40).toInt)
            val subGoal2 = SampleGoal(network.networkFull + " FactContent ", 15 + (Math.random() * 40).toInt)
            subGoal.addDependency(subGoal2)
            subGoal2.addDependency(waitForKsuidMapping)
            tlGoal.addWitnessRule(qualifyByNetwork(network.networkAbbr), subGoal)
        }

        val classifier = SampleGoal("Wiki Classifiers", 7)
        tlGoal.addDependency(classifier).declareTopLevel()

    }

    def qualifyByNetwork(networkAbbr: String): (Witness => Witness) = {
        w: Witness =>
            w.update(VariableAssignment[String](networkAbbrVar, networkAbbr))
    }

}

class WorkAroundTrack extends Track(TrackDescriptor("Sample"))
