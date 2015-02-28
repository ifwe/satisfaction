package satisfaction
package hadoop.hive

import _root_.org.joda.time.format.DateTimeFormat
import _root_.org.joda.time.Days
import _root_.org.joda.time.DateTime
import scala.io.Source
import _root_.org.apache.hadoop.hive.ql.metadata.HiveException
import _root_.org.apache.hadoop.hive.conf.HiveConf
import util.Releaseable

case class HiveSatisfier(val queryResource: String, val conf : HiveConf)( implicit val track : Track) 
///      extends Satisfier with MetricsProducing with Progressable with Logging with java.io.Closeable {
      ////extends Satisfier with MetricsProducing  with Logging with java.io.Closeable {
      extends Satisfier  with Logging with java.io.Closeable {

   override def name = s"Hive( $queryResource )" 
  
   
   val  driver  = new Releaseable[HiveDriver]( {
        info(s"Creating  HiveLocalDriver ${Thread.currentThread().getName()} ")
	    HiveDriver(conf)
   }  )
   
   /** 
    *   For closeable 
    */
   override def close() = {
       info(s" Closing Hive Satisfier")
       if( driver.isBuilt) {
         driver.get.close 
         driver.release
       } else  {
         warn(" Attempting to close already closed Driver ")
       }
   }
   

    def executeMultiple( hql: String, allProps : Witness): Boolean = {
        queries(hql).foreach(query => {
            if (query.trim.length > 0) {
                info(s" Executing query $query")
                val results = execute(query.trim, allProps)
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
    /**
    def progressCounter : ProgressCounter = {
       driver.get match {
         case pr : Progressable => pr.progressCounter
         case _  => { throw new RuntimeException(" HiveDriver needs to implement Progressable !!! ") }
      }
    }
    * 
    */
    
    def queryTemplate : String = {
       if( queryResource.endsWith(".hql"))  { 
         if(track.hasResource( queryResource)) {
           track.getResource( queryResource) 
         } else {
           io.Source.fromInputStream(  this.getClass().getClassLoader().getResourceAsStream( queryResource) ).mkString
         }
       } else {
         queryResource
       }
    }
    
    def loadSetup( allProps : Witness ) : ExecutionResult = {
         val setupScript = track.getResource("setup.hql")
         info(s" Running setup script $setupScript")
         substituteAndExecQueries( setupScript, allProps) 
    }
    
    
    def queries( hql: String) : Seq[String] = {
       hql.split(";")
    }
    
       
    def sourceFile( resourceName : String, allProps : Witness ) : Boolean = {
       info(s" Sourcing resource ## $resourceName ##")
       if( track.hasResource( resourceName ) ) {
           val readFile= track.getResource( resourceName) 
           info(s" ## Processing SourceFile ## $readFile")
           val result = substituteAndExecQueries( readFile, allProps)
           result.isSuccess
       } else {
          warn(s"No resource $resourceName available to source.") 
          false
       }
    }
    
    

    def execute( queryUnclean : String, allProps : Witness ) : Boolean = {
   	  val query : String = HiveSatisfier.stripComments( queryUnclean)
      if(query.length == 0 ) {
    	return true;
      }
      info(s"HIVE_SATISFIER :: Executing Query $query")
      if (query.trim.toLowerCase.startsWith("set")) {
    	val setExpr = query.trim.split(" ")(1)
    	val kv = setExpr.split("=")
    	//// add escaping ???
        info(s" Setting configuration ${kv(0).trim} to ${kv(1)} ")
    	driver.get.setProperty( kv(0).trim, kv(1))
    	return true
      }
      if( query.trim.toLowerCase.startsWith("source") ) {
    	val cmdArr = query.split(" ")
    	if(cmdArr.size != 2 ) {
    	  warn(s" Unable to interpret source command $query ")
    	  return false 
    	} else {
    	  return sourceFile(cmdArr(1).replaceAll("'","").trim, allProps)
   		}
      }
      val result = driver.get.executeQuery( query)
      if( result == false ) {
        driver.get.close
        driver.release
      }
      info(s"HIVE_SATISFIER : Driver returned $result")
      result
    }

    
    def substituteAndExecQueries( queryString : String, allProps : Witness) : ExecutionResult = {
      /// XXX If source statements 
      ///  propagate variables to driver ...
        val execResult = new ExecutionResult(queryString)
        val queryMatch = Substituter.substitute(queryString, allProps) match {
            case Left(badVars) =>
                error(s" Missing variables in query Template $queryString")
                badVars.foreach { s => error(s"   Missing variable ${s} ") }
                execResult.markFailure( s"Missing variables in queryTemplate ${badVars.mkString(",")} ")
            case Right(query) =>
                try {
                    info(" Beginning executing Hive queries ..")
                    val result=  executeMultiple( query, allProps)
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
      info(s"HiveSatisfier satisfying $params ") 
      val allProps = track.getTrackProperties(params)
      driver.get
      info(s" Track Properties is $allProps ; Witness is $params ")
      if( track.hasResource("setup.hql")) {
           val setupResult = loadSetup(allProps)
           if( ! setupResult.isSuccess) {
              return setupResult
           }
       }
       var res = substituteAndExecQueries(queryTemplate, allProps)

         driver.get match {
            case mp : MetricsProducing => {
              _jobMetrics = mp.jobMetrics
            }  
            case _  => {  info(" Driver is not MetricsProducing" )  }
         }

         driver.get.close
         driver.release

         res
    }

    
    @Override 
    override def abort() : ExecutionResult =  robustly {
      if(driver.isBuilt ) {
       driver.get.abort
       driver.get.close
       driver.release

      }
       true
    }
    
    
   ///
    private var _jobMetrics = new MetricsCollection(s" Hive Query $queryResource ")
    def jobMetrics : MetricsCollection =  {
      /**
       if( driver.isBuilt && driver.get.isInstanceOf[MetricsProducing])  {
           val mpDriver = driver.get.asInstanceOf[MetricsProducing] 
            _jobMetrics = mpDriver.jobMetrics
            
       }
       * 
       */
       _jobMetrics
    }
   
   
}

object HiveSatisfier {
    val HiveCommentDelimiter = "---"

  /**
   *  Strip out comments starting with  ---,
   *  So that we can have comments in our Hive scripts ..
   *   #KillerApp
   */ 
  def stripComments( queryString : String ) : String = {
    queryString.split("\n").map( line => {
       if( line.contains(HiveCommentDelimiter) ) {
          line.substring( 0, line.indexOf(HiveCommentDelimiter)) 
       } else {
          line  
       }
     }
    ).mkString("\n").trim
  }
  
  
}

