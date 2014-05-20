package com.klout
package satisfaction
package hadoop
package hive

import org.joda.time.format.DateTimeFormat
import org.joda.time.Days
import org.joda.time.DateTime
import scala.io.Source
import org.apache.hadoop.hive.ql.metadata.HiveException

case class HiveSatisfier(queryResource: String, driver: HiveDriver)( implicit val track : Track) extends Satisfier with MetricsProducing {

   override def name = s"Hive( $queryResource )" 
  
   val execResult = new ExecutionResult( queryTemplate, new DateTime)

    def executeMultiple(hql: String): Boolean = {
        val multipleQueries = hql.split(";")
        multipleQueries.foreach(query => {
            if (query.trim.length > 0) {
                println(s" Executing query $query")
                val results = driver.executeQuery(query)
                if (!results)
                    return results

            }

        })
        true
    }
    
    
    def queryTemplate : String = {
       println(" Query Template -- query Resource is " + queryResource)      
       if( queryResource.endsWith(".hql"))  { 
          track.getResource( queryResource) 
       } else {
         queryResource
       }
    }
    
    def loadSetup = {
      try {
        val setupScript = track.getResource("setup.hql")
        if( setupScript != null) {
          println(s" Running setup script $setupScript")
          if ( !executeMultiple( setupScript) ) {
            throw new HiveException("Trouble loading setup.hql")
          }
        }
        
      } catch { 
        case ill : IllegalArgumentException =>
          println("Unable to find setup.hql")
        case unexpected:Throwable  =>
         throw unexpected 
      }
      
    }
    

    @Override
    override def satisfy(params: Witness): ExecutionResult = {
      try {

        val allProps = track.getTrackProperties(params)
        println(s" Track Properties is $allProps ")
        /// XXX need to set Auxjars later 
        if( driver.isInstanceOf[HiveLocalDriver]) {
          val lDriver = driver.asInstanceOf[HiveLocalDriver]
          ///lDriver.setAuxJarFolder( track.auxJarFolder )
          
        }
        val queryMatch = Substituter.substitute(queryTemplate, allProps) match {
            case Left(badVars) =>
                println(" Missing variables in query Template ")
                badVars.foreach { s => println("  ## " + s) }
                execResult.markFailure
                execResult.errorMessage = "Missing variables in queryTemplate " + badVars.mkString(",")
            case Right(query) =>
                val startTime = new DateTime
                try {
                    loadSetup
                    println(" Beginning executing Hive queries ..")
                    val result=  executeMultiple(query)
                    execResult.metrics.mergeMetrics( jobMetrics)
                    if( result ) { execResult.markSuccess } else { execResult.markFailure }
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
          System.out.println(" Unexpected !!! "+ unexpected)
          unexpected.printStackTrace( System.out)
          unexpected.printStackTrace( System.err)
          execResult.markUnexpected( unexpected)
      }
    }
    
    @Override 
    override def abort() : ExecutionResult =  {
      
      driver.abort
      execResult.markFailure 
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

