package satisfaction
package engine
package actors


import satisfaction.ExecutionResult
import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Future._
import scala.concurrent._
import scala.util.Try
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.File
import org.joda.time.DateTime
import scala.util.Success
import scala.util.Failure

/**
 *  JobRunner actually satisfies a goal,
 *   most likely by running some sort of job.
 */
class JobRunner(
    val satisfier: Satisfier,
    val track : Track,
    val goal : Goal, 
    val witness : Witness,
    params: Witness ) extends Actor with ActorLogging {

  
    implicit val executionContext : ExecutionContext = ExecutionContext.global
    
    private var _messageSender: ActorRef = null
    def messageSender = _messageSender

    private var _startTime : DateTime = null;
    def startTime = _startTime

    LogWrapper.modifyLogger( track)
    LogWrapper.modifyLogger( goal)
    LogWrapper.modifyLogger( satisfier)
    val logger = new LogWrapper[ExecutionResult]( track, goal, witness)

    def receive = {
        case Satisfy =>
            log.info(s"Asked to satisfy ${track.descriptor.trackName} :: ${goal.name} for params: ${params} by ${sender.path} ")
            _startTime = DateTime.now

                val satisfierFuture = future {
                    logger.log { () => 
                          JobRunner.threadPreserve {  () => { 
                                val result = satisfier.satisfy(params)
                                satisfier match {
                                  case closer : java.io.Closeable => {
                                     logger.info(s" Closing Closable Satisfier $satisfier for Goal ${goal.name} and Witness $witness ") 
                                     closer.close()
                                  }
                                  case _ =>  /// don't need to close anything ...
                                }
                                result
                            } 
                          }
                    } match {
                      case Success(execResult) =>
                        log.info(s" JobRunner ${goal.name} $witness received ExecResult ${execResult.isSuccess} ")
                        logger.info(s" JobRunner ${goal.name} $witness received ExecResult ${execResult.isSuccess} ")
                        execResult.hdfsLogPath = logger.hdfsLogPath.toString
                        execResult
                      case Failure(throwable) =>
                        //// Error occurred somehow because of logging,
                        ///   or from satisfier throwing unexpected exception
                        log.info(s" JobRunner ${goal.name} $witness received Unexpected  ${throwable} :: ${throwable.getMessage()} ")
                        logger.info(s" JobRunner ${goal.name} $witness received Unexpected  ${throwable} :: ${throwable.getMessage()} ")
                        val execResult2 = new ExecutionResult(goal.name, _startTime )
                        execResult2.hdfsLogPath = logger.hdfsLogPath.toString
                        execResult2.markUnexpected( throwable)
                    }
                }
                log.info(s" JobRunner ... MessageSender is $sender ")
                _messageSender = sender
                satisfierFuture onComplete {
                    log.info(s" Job Runner :: Future OnComplete ${goal.name} ${witness}")
                    checkResults(_)
                }
        case Abort =>
          log.warning(s" Aborting Job ${goal.name} !!!")
          Console.println(s" Aborting Job ${goal.name} !!!")
          /// For now Aborts are fire and forget
          ///  Assume Abort has completed.
          //// TODO Wait for abort result, and send to parents

          val abortResult : ExecutionResult = satisfier.abort()
          markEvidence( _.markIncomplete )
          log.info( "Result of Abort Attempt is " + abortResult)
          Console.println( "Result of Abort Attempt is " + abortResult)
          finish()
          /**
          try {
             val abortResult : ExecutionResult = satisfier.abort()
              checkResults( Success(abortResult))
          } catch {
            case unexpected : Throwable =>
              checkResults( Failure(unexpected))
          }
          * 
          */
          
    }
    
    /**
     * If our DataInstances implement the "Markable" trait,
     *   Mark them as either completed or incomplete,
     *    based on the outcome of the job
     */
    def markEvidence( f: ( Markable => Unit)) = {
      goal.evidenceForWitness( witness).filter( _.isInstanceOf[DataOutput]).
          map( _.asInstanceOf[DataOutput].getDataInstance(witness) ).
          filter( _.isDefined).map( _.get ).
           foreach( di => {
             log.info(s" Check Markable !!! DataInstance ${di} with witness $witness ")
             di  match {
               case mk : Markable =>
                   log.info(s" Marking $mk with function $f ")
                   f(mk)
               case _  =>
                   log.info(s" $di is not markable")
             }          
          })
    }
    

    def checkResults(result: Try[ExecutionResult]) = {
        if (result.isSuccess) {
            val execResult = result.get
            execResult.hdfsLogPath = logger.hdfsLogPath.toString
             log.info(s" JobRunner ${goal.name} $witness result isSuccess = ${execResult.isSuccess} ")
            if (execResult.isSuccess ) {
                log.info(s" Sending JobRunSuccess to $messageSender ")
                markEvidence( _.markCompleted )
                messageSender ! new JobRunSuccess(execResult)
            } else {
                log.info(s" Sending JobRunFailed to $messageSender ")
                markEvidence( _.markIncomplete)
                messageSender ! new JobRunFailed(execResult)
            }
        } else {
             //// Error occurred with Akka Actors
            log.info(s" JobRunner ${goal.name} $witness result isFailure ; Unexpected Error  " + result.failed.get)
            val execResult = new ExecutionResult( "Failure executing Goal " + goal.name , new DateTime)
            execResult.hdfsLogPath = logger.hdfsLogPath.toString
            execResult.markUnexpected( result.failed.get)
                log.info(s" Sending JobRunFailed unexpected to $messageSender ")
            messageSender ! new JobRunFailed(execResult)
        }
        finish()
    }
    
    /**
     *  Job has finished ...
     */
    def finish() = {
      log.info(s" Finishing up !! ${self.path} ")
       context.system.stop( self) 
    }

}

object JobRunner {
  
  
    /**
     *  Since Akka uses the context ClassLoader, make sure 
     *    the task doesn't overwrite the value Akka expects to be there,
     *    or else messages will get lost..
     */
    def threadPreserve[T]( functor : () => T ) : T = {
       val thClBefore = Thread.currentThread.getContextClassLoader
       val res = functor()
       val thClAfter = Thread.currentThread.getContextClassLoader
       if( thClBefore != thClAfter)  {
         Thread.currentThread.setContextClassLoader(thClBefore)
       }
       res
    }
  
}
