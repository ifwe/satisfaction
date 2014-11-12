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
  
   
   lazy val driver : HiveDriver = {
	   HiveDriver(conf)
   } 
   

    def executeMultiple( hql: String): Boolean = {
        queries(hql).foreach(query => {
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
    
    def loadSetup( allProps : Witness ) : ExecutionResult = {
         val setupScript = track.getResource("setup.hql")
         info(s" Running setup script $setupScript")
         /// XXX Merge multiple Execution results together ...
         substituteAndExecQueries( setupScript, allProps) 
    }
    
    
    def queries( hql: String) : Seq[String] = {
       hql.split(";")
    }
    
    def substituteAndExecQueries( queryString : String, allProps : Witness) : ExecutionResult = {
      /// XXX If source statements 
      ///  propagate variables to driver ...
        val execResult = new ExecutionResult(queryString)
        val queryMatch = Substituter.substitute(queryString, allProps) match {
            case Left(badVars) =>
                error(" Missing variables in query Template ")
                badVars.foreach { s => error(s"   Missing variable ${s} ") }
                execResult.markFailure( s"Missing variables in queryTemplate ${badVars.mkString(",")} ")
            case Right(query) =>
                try {
                    info(" Beginning executing Hive queries ..")
                    val result=  executeMultiple( query)
                    //// XXX refactor to get each individual query
                    execResult.metrics.mergeMetrics( jobMetrics)
                    if( result ) { execResult.markSuccess() } else { execResult.markFailure() }
                } catch {
                    case unexpected : Throwable =>
                        println(s" Unexpected error $unexpected")
                        error(s" Unexpected error $unexpected", unexpected)
                        unexpected.printStackTrace()
                        execResult.markUnexpected(unexpected)
                }
        }
        return execResult
    }

    @Override
    override def satisfy(params: Witness): ExecutionResult = {
        val allProps = track.getTrackProperties(params)
        info(s" Track Properties is $allProps ; Witness is $params ")
        if( track.hasResource("setup.hql")) {
           val setupResult = loadSetup(allProps)
           if( ! setupResult.isSuccess) {
              return setupResult
           }
        }
        substituteAndExecQueries(queryTemplate, allProps)
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

