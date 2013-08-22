package com.klout.satisfaction
package executor
package actors

import scalaxb._
import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime

class ProofEngineSpec extends Specification {
    object NetworkAbbr extends Param[String]("network_abbr")
    object DoDistcp extends Param[Boolean]("doDistcp")
    object runDate extends Param[String]("dt")

    "ProofEngineSpec" should {
        "get a goals status" in {
            val engine = new ProofEngine()
            val vars: Set[Param[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)

            val project = new Project("SimpleProject",
                Set(singleGoal),
                null,
                null)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            ///val result = engine.satisfyProject(project, witness)
            val status = engine.getStatus(singleGoal, witness)
            println(status.state)

            status.state must_== GoalState.Unstarted
        }

        "satisfy a single goal" in {
            val engine = new ProofEngine()
            val vars: Set[Param[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)

            val project = new Project("SimpleProject",
                Set(singleGoal),
                null,
                null)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoal(singleGoal, witness)
            println(result.state)
            result.state must_== GoalState.Success
        }

        "satisfy a goal hierarchy" in {
            val engine = new ProofEngine()
            val vars: Set[Param[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)
            val dep1 = TestGoal("Child1", vars)
            val dep2 = TestGoal("Child2", vars)
            val dep3 = TestGoal("Child3", vars)
            singleGoal.addDependency(dep1).addDependency(dep2).addDependency(dep3)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoal(singleGoal, witness)
            println(result.state)
            result.state must_== GoalState.Success
        }

        "satisfy a  three level goal hierarchy" in {
            val engine = new ProofEngine()
            val vars: Set[Param[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)
            val dep1 = TestGoal("Child1", vars)
            val dep2 = TestGoal("Child2", vars).addDependency(TestGoal("Grand2_1", vars))

            val dep3 = TestGoal("Child3", vars).addDependency(TestGoal("Grand3_1", vars)).addDependency(TestGoal("Grand3_2", vars))
            singleGoal.addDependency(dep1).addDependency(dep2).addDependency(dep3)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoal(singleGoal, witness)
            println(result.state)
            result.state must_== GoalState.Success
        }

        "satisfy a single slow goal" in {
            val engine = new ProofEngine()
            val vars: Set[Param[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal.SlowGoal("SlowGoal", vars, 6, 5000)

            val project = new Project("SimpleSlowProject",
                Set(singleGoal),
                null,
                null)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoalBlocking(singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            result.state must_== GoalState.Success
        }

        "fail a single failing goal" in {
            val engine = new ProofEngine()
            val vars: Set[Param[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal.FailedGoal("FailingGoal", vars, 0, 0)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoalBlocking(singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            result.state must_== GoalState.Failed
        }

        "fail a slow  single failing goal" in {
            val engine = new ProofEngine()
            val vars: Set[Param[_]] = Set(NetworkAbbr, runDate)
            val singleGoal = TestGoal.SlowGoal("SlowFailingGoal", vars, 10, 2000)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoalBlocking(singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            result.state must_== GoalState.Failed
        }

        "fail a grandchild   single failing goal" in {
            val engine = new ProofEngine()
            val vars: Set[Param[_]] = Set(NetworkAbbr, runDate)
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
            val result = engine.satisfyGoalBlocking(parentGoal, witness, Duration(60, SECONDS))
            println(result.state)
            result.state must_== GoalState.DepFailed

        }

    }
}