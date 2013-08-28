package com.klout.satisfaction
package executor

import actors.ProofEngine
import actors.GoalStatus

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Success
import scala.util.Failure
import scala.util.Try

/**
 *  Main class for running a Goal or Project
 *   to use as a command line utility
 */
object Satisfaction {
    val engine = new ProofEngine()

    def satisfyGoal(goal: Goal, witness: Witness) {

        val fStatus = engine.satisfyGoal(goal, witness)

        Iterator.continually(Await.ready(fStatus, Duration.Inf)).takeWhile(!_.isCompleted).foreach { f =>
            println("Waiting on Future" + f)
        }

        fStatus.value match {
            case Some(something) => something match {
                case tr: Try[GoalStatus] =>
                    tr match {
                        case Success(s) =>
                            println(" Success  is " + s.state)
                        case Failure(f) =>
                            println("Failure " + f)
                            f.printStackTrace()
                    }
            }
            case None =>
                println("NONE Something bad happened???")
        }
    }

    def main(argv: Array[String]): Unit = {

        println(" before continually")
        Iterator.continually(Thread.sleep(2000)).foreach { _ =>
            println("BLAH ")
        }

        println(" After continually")
    }

}