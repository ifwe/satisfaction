package com.klout.satisfaction
package executor
package actors

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits._
import scala.concurrent.Future
import scala.concurrent.Future._
import scala.concurrent._
import scala.util.Try
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.File

class JobRunner(
    satisfier: Satisfier,
    params: Substitution) extends Actor with ActorLogging {

    var satisfierFuture: Future[Boolean] = null
    var messageSender: ActorRef = null

    def receive = {
        case Satisfy =>
            log.info(s"Asked to satisfy for params: $params.mkString")

            if (satisfierFuture == null) {
                satisfierFuture = future {
                    val currOut = Console.out
                    val currErr = Console.err
                    val outStream = getLoggingOutput
                    try {
                        Console.setOut(outStream)
                        Console.setErr(outStream)
                        satisfier.satisfy(params)
                    } catch {
                        case t: Throwable =>
                            log.error(t, "Unexpected Error while running job")
                            t.printStackTrace(currErr)
                            t.printStackTrace(new java.io.PrintWriter(outStream))

                            false
                    } finally {
                        outStream.flush()
                        outStream.close()
                        Console.setOut(currOut)
                        Console.setOut(currErr)
                    }
                }
                messageSender = sender
                satisfierFuture onComplete {
                    checkResults(_)
                }
            }

    }

    def getLoggingOutput: OutputStream = {
        //// XXX TODO
        //// Come up with reasonable naming convention for log files ...
        new FileOutputStream(new File(params.raw.mkString("_").replace(" ", "_").replace("->", "_")))
    }

    def checkResults(result: Try[Boolean]) = {
        log.info("Sending GoalSatisfied to parent")
        log.info("Some result =  " + result)
        if (result.isSuccess) {
            if (result.get) {
                messageSender ! GoalSatisfied
            } else {
                log.info(" Bool is false " + GoalFailed)
                messageSender ! GoalFailed
            }
        } else {
            log.info(" result isFailure " + result.failed.get)
            messageSender ! GoalFailed
        }
    }

}
