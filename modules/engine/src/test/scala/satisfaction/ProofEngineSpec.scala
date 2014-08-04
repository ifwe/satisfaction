package satisfaction
package engine
package actors

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
import satisfaction.engine._
import scala.concurrent.Await
import akka.util.Timeout


class ProofEngineSpec extends Specification {
    val NetworkAbbr = new Variable[String]("network_abbr", classOf[String])
    val DoDistcp = new Variable[Boolean]("doDistcp", classOf[Boolean])
    val runDate = new Variable[String]("dt", classOf[String])
    val timeout : Timeout = new Timeout(Duration.create(300, "seconds"));

    implicit val track : Track = new Track( TrackDescriptor("TestTrack"))

    "ProofEngineSpec" should {

      /**
        "get a goals status" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            ///val result = engine.satisfyProject(project, witness)
            val status = engine.getStatus(singleGoal, witness)
            println(status.state)

            ///engine.stop
            status.state must_== GoalState.Unstarted
        }
        "satisfy a single goal" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val resultFuture : Future[GoalStatus] = engine.satisfyGoal( singleGoal, witness)
            var isDone = false
            resultFuture.onComplete( { 
              case Success(status) => println(status.state);
                 System.out.println(" COMPLETED !!!")
              	status.state must_== GoalState.Success
              	 isDone = true
              case Failure(t) =>
                t.printStackTrace()
                isDone=true
                true must_== false
            } )

            val status = Await.result(resultFuture, timeout.duration )
           	status.state must_== GoalState.Success

        }

        "satisfy a goal hierarchy" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)
            val dep1 = TestGoal("Child1", vars)
            val dep2 = TestGoal("Child2", vars)
            val dep3 = TestGoal("Child3", vars)
            singleGoal.addDependency(dep1).addDependency(dep2).addDependency(dep3)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            System.out.println(" Before Satisfy")
            val resultFuture = engine.satisfyGoal( singleGoal, witness)
              resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.Success
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )

            val status = Await.result(resultFuture, timeout.duration )
           	status.state must_== GoalState.Success
        }

        "satisfy a goal deeply nested hierarchy" in {
            val engine = new ProofEngine()
            //// All Goals created now should get the same track 
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
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
            val resultFuture = engine.satisfyGoal(singleGoal, witness)
              resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.Success
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )
            

            val status = Await.result(resultFuture, timeout.duration )
           	status.state must_== GoalState.Success
        }

        "satisfy a  three level goal hierarchy" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)
            val dep1 = TestGoal("Child1", vars)
            val dep2 = TestGoal("Child2", vars).addDependency(TestGoal("Grand2_1", vars))

            val dep3 = TestGoal("Child3", vars).addDependency(TestGoal("Grand3_1", vars)).addDependency(TestGoal("Grand3_2", vars))
            singleGoal.addDependency(dep1).addDependency(dep2).addDependency(dep3)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val resultFuture = engine.satisfyGoal(singleGoal, witness)
              resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.Success
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )
            ///resultFuture.wait
        }

        "satisfy a single slow goal" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal.SlowGoal("SlowGoal", vars, 6, 5000)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoalBlocking(singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            ///engine.stop
            result.state must_== GoalState.Success
        }

        "fail a single failing goal" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal.FailedGoal("FailingGoal", vars, 0, 0)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoalBlocking(singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            ///engine.stop
            result.state must_== GoalState.Failed
        }

        "fail a slow  single failing goal" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal.FailedGoal("SlowFailingGoal", vars, 10, 2000)

            val witness = Witness((runDate -> "20130816"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoalBlocking(singleGoal, witness, Duration(60, SECONDS))
            println(result.state)
            ///engine.stop
            result.state must_== GoalState.Failed
        }

        "fail a grandchild   single failing goal" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
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
            ///engine.stop
            result.state must_== GoalState.DependencyFailed

        }

        "diamond hierarchy successful goal " in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
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
            val result = engine.satisfyGoalBlocking(parentGoal, witness, Duration(60, SECONDS))
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
            val vars: List[Variable[_]] = List(runDate)
            val vars2: List[Variable[_]] = List(runDate, NetworkAbbr)

            val parentGoal = TestGoal.SlowGoal("TopGoal", vars, 5, 1000)

            val child = TestGoal.SlowGoal("WitnessMapped", vars2, 2, 1000)
            parentGoal.addWitnessRule(getNetworkMapper("ig"), child)

            parentGoal.addWitnessRule(getNetworkMapper("fb"), child)

            parentGoal.addWitnessRule(getNetworkMapper("tw"), child)
            parentGoal.addWitnessRule(getNetworkMapper("fs"), child)
            parentGoal.addWitnessRule(getNetworkMapper("kl"), child)

            val witness = Witness((runDate -> "20130821")) 
            val result = engine.satisfyGoalBlocking( parentGoal, witness, Duration(60, SECONDS))
            println(result.state)

            ///engine.stop
            result.state must_== GoalState.Success

        }
        
        
        "satisfy a fan out goal" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(runDate)
            val vars2: List[Variable[_]] = List(runDate, NetworkAbbr)
            val networks : List[String] = List("ig","fb","tw","fs","kl")

            val child = TestGoal.SlowGoal("FanoutSubGoal", vars2, 2, 1000)
            val fanOut = FanOutGoal( child, NetworkAbbr, networks )
          
            val witness = Witness((runDate -> "20130821")) 
            val result = engine.satisfyGoalBlocking( fanOut, witness, Duration(60, SECONDS))
            println(result.state)

            ///engine.stop
            result.state must_== GoalState.Success

        }
        
        
        "already satisfied" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal.AlreadySatisfiedGoal("AlreadySatisfiedGoal", vars, 1, 1)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val resultFuture : Future[GoalStatus] = engine.satisfyGoal(singleGoal, witness)
            println(" Result Future is " + resultFuture)
            resultFuture.onComplete( { 
              case Success(status) => println(status.state);
              	status.state must_== GoalState.AlreadySatisfied
              case Failure(t) =>
                t.printStackTrace()
                true must_== false
            } )
            
            val status =  Await.result( resultFuture , timeout.duration)
           	status.state must_== GoalState.AlreadySatisfied

        }
        
        "already satisfied blocking" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal.AlreadySatisfiedGoal("AlreadySatisfiedBlocking", vars, 1, 1)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result : GoalStatus = engine.satisfyGoalBlocking( singleGoal, witness, Duration(10, SECONDS))
            println(" Result is " + result)
             result.state must_== GoalState.AlreadySatisfied
        }
        
         "satisfy a goal hierarchy with one already satisfied" in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val singleGoal = TestGoal("SimpleGoal", vars)
            val dep1 = TestGoal("Child1", vars)
            val dep2 = TestGoal.AlreadySatisfiedGoal("SatisfiedChild2", vars, 1, 1)
            ///val dep2 = TestGoal("Child2", vars)
            val dep3 = TestGoal("Child3", vars)
            singleGoal.addDependency(dep1).addDependency(dep2).addDependency(dep3)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val result = engine.satisfyGoalBlocking( singleGoal, witness, Duration( 20, SECONDS))
             result.state must_== GoalState.Success
        }
         
         
        "abort a goal"  in {
            val engine = new ProofEngine()
            val vars: List[Variable[_]] = List(NetworkAbbr, runDate)
            val longRunningGoal = TestGoal.SlowGoal("AbortedGoal", vars, 5000, 5000)

            val witness = Witness((runDate -> "20130815"), (NetworkAbbr -> "tw"))
            val resultFuture : Future[GoalStatus] = engine.satisfyGoal( longRunningGoal, witness)
            resultFuture.onComplete( { 
              case Success(status) => 
                 println(" Success ???  State us " + status.state)
                 //// No exceptions were thrown, so job was aborted
           	     status.state must_== GoalState.Aborted
              case Failure(t) =>
                true must_== false
            } )
            println(" Result Future is "+ resultFuture)
          
            Thread.sleep( 2000)
            println(" Before Abort Goal")

            engine.abortGoal(longRunningGoal, witness)
            println(" After Abort Goal")
            

            val status = Await.result(resultFuture, Duration( 10 , SECONDS) )
           	status.state must_== GoalState.Aborted

        }
        
        **/
        
        "Recursive Reharvest" in {
            val engine = new ProofEngine()
            val dtVar = Variable("dt")
            
            
            val wit =  Witness((dtVar -> "20140711"))

            println( s" Wit is $wit ")

            
            val reharvestGoal = TestGoal.ReharvestGoal( "20140701")
            
            val res = engine.satisfyGoalBlocking( reharvestGoal, wit, Duration(60, SECONDS))
            ///val resFuture = engine.satisfyGoal( reharvestGoal, wit)
          
            
            
            true
        }

    }
}