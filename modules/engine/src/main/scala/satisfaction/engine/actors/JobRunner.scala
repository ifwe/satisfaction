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
import satisfaction.RobustRun
import java.util.concurrent.ForkJoinPool

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

  
    //// Create our own Thread pool for running our own jobs...
    ////  rather then mess with the Akka or play threads
    lazy implicit val executionContext : ExecutionContext = {
       val pool =  new ForkJoinPool() /// Configure the ForkJoinPool
       ExecutionContext.fromExecutor( pool)
    }
    
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
                    logger.log { 
                       threadPreserve { 
                          satisfier.satisfy(params)
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
                    log.info(s" Job Runner :: Future OnComplete ${goal.name} ${witness} ${satisfier}")
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
    def markEvidence( f: ( Markable => Unit)) : Boolean = JobRunner.threadPreserve(goal.track) {
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
      true
    }
    

    def checkResults(result: Try[ExecutionResult]) = {
        if (result.isSuccess) {
            val execResult = result.get
            execResult.hdfsLogPath = logger.hdfsLogPath.toString
             log.info(s" JobRunner ${goal.name} $witness result isSuccess = ${execResult.isSuccess} ")
             /// Need to handle 
            if (execResult.isSuccess ) {
                log.info(s" Sending JobRunSuccess to $messageSender ")
                val markRes  = RobustRun( s"MarkResult ${execResult.executionName} ", {  markEvidence( _.markCompleted )  } )
                if( markRes.isSuccess) {
                   messageSender ! new JobRunSuccess(execResult)
                } else {
                   //// There was an error while trying to mark the DataInstances ...
                   log.error(s" Error while trying to mark evidence ${markRes.errorMessage} ", markRes.stackTrace)
                   messageSender ! new JobRunFailed(markRes)
                }
            } else {
                log.info(s" Sending JobRunFailed to $messageSender ")
                val markRes  = RobustRun( s"MarkResult ${execResult.executionName} ", {  markEvidence( _.markCompleted )  } )
                //// If there is an error , send the previous fail result
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
      /// If the satisfier has any resources which needs to be released,
      ////  call close()
      satisfier match {
         case closer : java.io.Closeable => {
             logger.info(s" Closing Closable Satisfier $satisfier for Goal ${goal.name} and Witness $witness ") 
             closer.close()
         }
         case _ =>  { logger.info(s" Satisfier $satisfier is not closeable") }
      }
      if( context != null && self != null) {
        log.info(s" Finishing up !! ${self.path} ")
        context.system.stop( self) 
      } else {
        log.warning(s" Call to finish called with null context $context -- self = $self ")
      }
    }

    /**
     *    Set the Thread Context Classloader to the classloader
     *     of the Satisfier, so that it has access to all the 
     *     classes that that Satisfier would
     */
    def threadPreserve[T]( functor :  => T ) : T =  JobRunner.threadPreserve(satisfier)(functor)

}  
  

object JobRunner {

    /**
     *  Since Akka uses the context ClassLoader, make sure 
     *    the task doesn't overwrite the value Akka expects to be there,
     *    or else messages will get lost..
     *    
     *    Set the Thread Context Classloader to the classloader
     *     of the Satisfier, so that it has access to all the 
     *     classes that that Satisfier would
     */
    def threadPreserve[T](clObj : Any)( functor :  => T ) : T = {
       val thClBefore = Thread.currentThread.getContextClassLoader
       val objCl = clObj.getClass.getClassLoader
       Thread.currentThread().setContextClassLoader(objCl)
       val res = functor
       val thClAfter = Thread.currentThread.getContextClassLoader
       if( thClBefore != thClAfter)  {
         println(s" Setting ${Thread.currentThread} ContextClassLoader back to ${thClBefore} from ${thClAfter} ")
       }
       Thread.currentThread.setContextClassLoader(thClBefore)
       res
    }
  
}
