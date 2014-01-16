package com.klout
package satisfaction
package engine

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

    def satisfyGoal( goal: Goal, witness: Witness) : GoalStatus = {
        val defaultTrackDesc = TrackDescriptor( goal.name )
        val defaultTrack =  new Track( defaultTrackDesc, Set[Goal]( goal ) )
        satisfyGoal( defaultTrack, goal, witness)
    }
    
    def satisfyGoal(track :Track, goal: Goal, witness: Witness) : GoalStatus = {

        val fStatus = engine.satisfyGoal(track, goal, witness)

        Iterator.continually(Await.ready(fStatus, Duration(300, SECONDS))).takeWhile(!_.isCompleted).foreach { f =>
            println("Waiting on Future" + f)
            val statuses = engine.getGoalsInProgress
            println(statuses.size + " goals are in progress ")
            statuses.foreach { status: GoalStatus =>
                println("   Status for Goal " + status.goalName + " for Witness " + status.witness + " is " + status.state)
            }
        }

        fStatus.value match {
            case Some(something) => something match {
                case tr: Try[GoalStatus] =>
                    tr match {
                        case Success(s) =>
                            return s
                        case Failure(f) =>
                            f.printStackTrace()
                            throw f
                    }
            }
            case None =>
                System.out.println("NONE Something bad happened???")
                null
        }
    }

    def main(argv: Array[String]): Unit = {

        println(" before continually")
        Iterator.continually(Thread.sleep(2000)).foreach { _ =>
            println("BLAH ")
            val statuses = engine.getGoalsInProgress
            println(statuses.size + " goals are in progress ")
            statuses.foreach { status: GoalStatus =>
                println("   Status for Goal " + status.goalName + " for Witness " + status.witness + " is " + status.state)
            }
        }

        println(" After continually")
    }

}