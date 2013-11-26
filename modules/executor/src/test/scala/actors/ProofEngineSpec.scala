package com.klout.satisfaction
package executor
package actors

import scalaxb._
import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import scala.util.Success
import scala.util.Failure


class ProofEngineSpec extends Specification {
    val NetworkAbbr = new Variable[String]("network_abbr", classOf[String])
    val DoDistcp = new Variable[Boolean]("doDistcp", classOf[Boolean])
    val runDate = new Variable[String]("dt", classOf[String])

    "ProofEngineSpec" should {
      
        "get a goals status" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            ///val result = engine.satisfyProject(project, witness)
            val track = Track(TrackDescriptor("TestTrack"), Set(singleGoal))
            val status = engine.getStatus(track, singleGoal, witness)
            println(status.state)

            ///engine.stop
            status.state must_== GoalState.Unstarted
        }


        "satisfy a single goal" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val track =  Track( TrackDescriptor("TestTrack"),  Set(singleGoal) )
            val resultFuture : Future[GoalStatus] = engine.satisfyGoal(track, singleGoal, witness)
            resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.Success
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )
            resultFuture.wait
        }

        "satisfy a goal hierarchy" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)
            val dep1 = TestGoal("Child1", vars)
            val dep2 = TestGoal("Child2", vars)
            val dep3 = TestGoal("Child3", vars)
            singleGoal.addDependency(dep1).addDependency(dep2).addDependency(dep3)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val track = Track( TrackDescriptor("TestTrack"), Set(singleGoal))
            val resultFuture = engine.satisfyGoal(track, singleGoal, witness)
              resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.Success
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )
            resultFuture.wait
        }

        "satisfy a goal deeply nested hierarchy" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("NestedGoal", vars)
            val dep1 = TestGoal("Level1", vars)
            val dep2 = TestGoal("Level2", vars)
            val dep3 = TestGoal("Level3", vars)
            val dep4 = TestGoal("Level4", vars)
            val dep5 = TestGoal("Level5", vars)
            val dep6 = TestGoal("Level6", vars)
            singleGoal.addDependency(dep1)
            dep1.addDependency(dep2)
            dep2.addDependency(dep3)
            dep3.addDependency(dep4)
            dep4.addDependency(dep5)
            dep5.addDependency(dep6)
            val witness = Witness((runDate -> "20130821"), (NetworkAbbr -> "ig"))
            val track = Track(TrackDescriptor("TestTrack"), Set(singleGoal))
            val resultFuture = engine.satisfyGoal(track,singleGoal, witness)
              resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.Success
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )
            resultFuture.wait
        }

        "satisfy a  three level goal hierarchy" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)
            val dep1 = TestGoal("Child1", vars)
            val dep2 = TestGoal("Child2", vars).addDependency(TestGoal("Grand2_1", vars))

            val dep3 = TestGoal("Child3", vars).addDependency(TestGoal("Grand3_1", vars)).addDependency(TestGoal("Grand3_2", vars))
            singleGoal.addDependency(dep1).addDependency(dep2).addDependency(dep3)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val track = Track(TrackDescriptor("TestTrack"), Set(singleGoal))
            val resultFuture = engine.satisfyGoal(track,singleGoal, witness)
              resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.Success
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )
            resultFuture.wait
        }

        "satisfy a single slow goal" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal.SlowGoal("SlowGoal", vars, 6, 5000)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val track = Track(TrackDescriptor("TestTrack"), Set(singleGoal))
            val result = engine.satisfyGoalBlocking(track,singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            ///engine.stop
            result.state must_== GoalState.Success
        }

        "fail a single failing goal" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal.FailedGoal("FailingGoal", vars, 0, 0)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val track = Track(TrackDescriptor("TestTrack"), Set(singleGoal))
            val result = engine.satisfyGoalBlocking(track,singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            ///engine.stop
            result.state must_== GoalState.Failed
        }

        "fail a slow  single failing goal" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal.FailedGoal("SlowFailingGoal", vars, 10, 2000)

            val witness = Witness((runDate -> "20130816"), (NetworkAbbr -> "tw"))
            val track = Track(TrackDescriptor("TestTrack"), Set(singleGoal))
            val result = engine.satisfyGoalBlocking(track,singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            ///engine.stop
            result.state must_== GoalState.Failed
        }

        "fail a grandchild   single failing goal" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val parentGoal = TestGoal.SlowGoal("SlowParentGoal", vars, 10, 2000)
            val child1 = TestGoal.SlowGoal("Child1", vars, 3, 2000)
            parentGoal.addDependency(child1)
            val child2 = TestGoal.SlowGoal("Child2", vars, 3, 2000)
            parentGoal.addDependency(child2)
            val child3 = TestGoal.SlowGoal("Child3", vars, 3, 2000)
            parentGoal.addDependency(child3)

            val grandChild1 = TestGoal.SlowGoal("fGrandChild1", vars, 3, 2000)
            child1.addDependency(grandChild1)

            val grandChild2 = TestGoal.FailedGoal("fGrandChild2", vars, 3, 2000)
            child2.addDependency(grandChild2)

            val grandChild3 = TestGoal.SlowGoal("fGrandChild3", vars, 3, 2000)
            child2.addDependency(grandChild3)

            val witness = Witness((runDate -> "20130818"), (NetworkAbbr -> "fb"))
            val track = Track(TrackDescriptor("TestTrack"), Set(parentGoal))
            val result = engine.satisfyGoalBlocking(track,parentGoal, witness, Duration(60, SECONDS))
            println(result.state)
            ///engine.stop
            result.state must_== GoalState.DependencyFailed

        }

        "diamond hierarchy successful goal " in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val parentGoal = TestGoal.SlowGoal("TopGoal", vars, 5, 1000)

            val child1 = TestGoal.SlowGoal("Child1", vars, 5, 1000)
            parentGoal.addDependency(child1)
            val child2 = TestGoal.SlowGoal("Child2", vars, 3, 1000)
            parentGoal.addDependency(child2)
            val child3 = TestGoal.SlowGoal("Child3", vars, 2, 1000)
            parentGoal.addDependency(child3)

            val baseGoal = TestGoal.SlowGoal("BaseGoal", vars, 4, 1000)
            child1.addDependency(baseGoal)
            child2.addDependency(baseGoal)
            child3.addDependency(baseGoal)

            val witness = Witness((runDate -> "20130810"), (NetworkAbbr -> "gp"))
            val track = Track(TrackDescriptor("TestTrack"), Set(parentGoal))
            val result = engine.satisfyGoalBlocking(track,parentGoal, witness, Duration(60, SECONDS))
            println(result.state)

            ///engine.stop
            result.state must_== GoalState.Success

        }

        def getNetworkMapper(net: String): (Witness => Witness) = {
            w: Witness =>
                w.update(NetworkAbbr -> net)
        }

        "witness mapping rule" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(runDate)
            val vars2: Set[Variable[_]] = Set(runDate, NetworkAbbr)

            val parentGoal = TestGoal.SlowGoal("TopGoal", vars, 5, 1000)

            val child = TestGoal.SlowGoal("WitnessMapped", vars2, 2, 1000)
            parentGoal.addWitnessRule(getNetworkMapper("ig"), child)

            parentGoal.addWitnessRule(getNetworkMapper("fb"), child)

            parentGoal.addWitnessRule(getNetworkMapper("tw"), child)
            parentGoal.addWitnessRule(getNetworkMapper("fs"), child)
            parentGoal.addWitnessRule(getNetworkMapper("kl"), child)

            val witness = Witness((runDate -> "20130821")) 
            val track = Track(TrackDescriptor("TestTrack"), Set(parentGoal))
            val result = engine.satisfyGoalBlocking(track,parentGoal, witness, Duration(60, SECONDS))
            println(result.state)

            ///engine.stop
            result.state must_== GoalState.Success

        }
        
        
        "already satisfied" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal.AlreadySatisfiedGoal("AlreadySatisfied", vars, 1, 1)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val track =  Track( TrackDescriptor("TestTrack"),  Set(singleGoal) )
            val resultFuture : Future[GoalStatus] = engine.satisfyGoal(track, singleGoal, witness)
            println(" Result Future is " + resultFuture)
            resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.AlreadySatisfied
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )
            if( ! resultFuture.isCompleted)
               resultFuture.wait
        }
        
        
             
        "already satisfied blocking" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal.AlreadySatisfiedGoal("AlreadySatisfied", vars, 1, 1)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val track =  Track( TrackDescriptor("TestTrack"),  Set(singleGoal) )
            val result : GoalStatus = engine.satisfyGoalBlocking(track, singleGoal, witness, Duration(10, SECONDS))
            println(" Result is " + result)
             result.state must_== GoalState.AlreadySatisfied
        }
        
         "satisfy a goal hierarchy with one already satisfied" in {
            val engine = new ProofEngine()
            val vars: Set[Variable[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)
            val dep1 = TestGoal("Child1", vars)
            val dep2 = TestGoal.AlreadySatisfiedGoal("SatisfiedChild2", vars, 1, 1)
            ///val dep2 = TestGoal("Child2", vars)
            val dep3 = TestGoal("Child3", vars)
            singleGoal.addDependency(dep1).addDependency(dep2).addDependency(dep3)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val track = Track( TrackDescriptor("TestTrack"), Set(singleGoal))
            val result = engine.satisfyGoalBlocking(track, singleGoal, witness, Duration( 20, SECONDS))
             result.state must_== GoalState.Success
        }

    }
}