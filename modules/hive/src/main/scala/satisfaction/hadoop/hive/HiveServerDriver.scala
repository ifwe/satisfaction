package satisfaction.hadoop.hive

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.service.HiveServer.HiveServerHandler
import org.apache.hadoop.hive.service.HiveServerException
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper

/**
 *  Try using the HiveServer interface to 
 *    execute commands ...
 */
class HiveServerDriver( val hiveConf : HiveConf ) extends HiveDriver with satisfaction.Logging {

    lazy private val hiveService : HiveServerHandler  = {
       new HiveServerHandler( hiveConf)
        
    }
  
    override def useDatabase(dbName: String) : Boolean  = {
        info(s"Using databaes $dbName")
        executeQuery( s"use $dbName") 
    }

    override def executeQuery(query: String): Boolean = {
       try {
         info(s" Executing query $query")
         hiveService.execute( query) 
         true
       } catch {
         case exc : HiveServerException  =>  {
           warn(s" Received Error while executing query ${exc.getMessage()} ; $query" )
          
           false
         }
         case unexpected : Throwable => {
           error(s" Unexpected error while executing query $query ", unexpected) 
           throw  unexpected
         } 
       }
      
    }
    
    override def abort()  = {
      info(" Aborting !!")
           /// Not sure this works with multiple Hive Goals ...
       /// Hive Driver is somewhat opaque
       info("HIVE_DRIVER Aborting all jobs for Hive Query ")
       HadoopJobExecHelper.killRunningJobs() 
    }
    
    override def close() = {
      hiveService.clean()
    }

}