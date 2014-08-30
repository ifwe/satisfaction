package satisfaction
package hadoop
package hive

import org.joda.time.format.DateTimeFormat
import org.joda.time.Days
import org.joda.time.DateTime
import scala.io.Source
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.conf.HiveConf

case class HiveSatisfier(val queryResource: String, val conf : HiveConf)( implicit val track : Track) 
      extends Satisfier with MetricsProducing with Progressable with Logging {

   override def name = s"Hive( $queryResource )" 
  
   val execResult = new ExecutionResult( queryTemplate, new DateTime)
   
   lazy val driver : HiveDriver = {
	   HiveDriver(conf)
   } 
   

    def executeMultiple(hql: String): Boolean = {
        val multipleQueries = hql.split(";")
        multipleQueries.foreach(query => {
            if (query.trim.length > 0) {
                info(s" Executing query $query")
                val results = driver.executeQuery(query.trim)
                if (!results)
                    return results
            }
        })
        true
    }
    
    /**
     *  Return the progressCounter from the HiveLocalDriver
     *   
     *  XXX Split up into multiple query and determine approximate runtime
     *    from track history
     *  XXX For now progress of currently running query is returned ...
     */
    def progressCounter : ProgressCounter = {
       driver match {
         case pr : Progressable => pr.progressCounter
         case _  => { throw new RuntimeException(" HiveDriver needs to implement Progressable !!! ") }
      }
    }
    
    def queryTemplate : String = {
       if( queryResource.endsWith(".hql"))  { 
          track.getResource( queryResource) 
       } else {
         queryResource
       }
    }
    
    def loadSetup = {
      if( track.hasResource("setup.hql")) {
         val setupScript = track.getResource("setup.hql")
         info(s" Running setup script $setupScript")
         if ( !executeMultiple( setupScript) ) {
            throw new HiveException("Trouble loading setup.hql")
         }
       }
    }
    

    @Override
    override def satisfy(params: Witness): ExecutionResult = {
      try {

        val allProps = track.getTrackProperties(params)
        info(s" Track Properties is $allProps ; Witness is $params ")
        val queryMatch = Substituter.substitute(queryTemplate, allProps) match {
            case Left(badVars) =>
                error(" Missing variables in query Template ")
                badVars.foreach { s => error(s"   Missing variable ${s} ") }
                execResult.markFailure( s"Missing variables in queryTemplate ${badVars.mkString(",")} ")
            case Right(query) =>
                val startTime = new DateTime
                try {
                    loadSetup
                    info(" Beginning executing Hive queries ..")
                    val result=  executeMultiple(query)
                    execResult.metrics.mergeMetrics( jobMetrics)
                    if( result ) { execResult.markSuccess() } else { execResult.markFailure() }
                } catch {
                    case unexpected : Throwable =>
                        println(s" Unexpected error $unexpected")
                        unexpected.printStackTrace()
                        val execResult = new ExecutionResult(query, startTime)
                        execResult.markUnexpected(unexpected)
                }
        }
        return execResult 
      } catch { 
        case unexpected : Throwable =>
          error(s" Unexpected Error :: ${unexpected.getLocalizedMessage} ", unexpected)
          execResult.markUnexpected( unexpected)
      }
    }
    
    @Override 
    override def abort() : ExecutionResult =  robustly {
       driver.abort
       true
    }

      
    
   ///
    def jobMetrics : MetricsCollection =  {
       if( driver.isInstanceOf[MetricsProducing])  {
           val mpDriver = driver.asInstanceOf[MetricsProducing] 
            mpDriver.jobMetrics
       } else {
         new MetricsCollection("Hive Query")
       }
    }
   
    
   
}

