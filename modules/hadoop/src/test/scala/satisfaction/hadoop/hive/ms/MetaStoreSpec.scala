package satisfaction
package hadoop
package hive.ms

import org.specs2.mutable._
import satisfaction.Witness
import org.joda.time._
import satisfaction._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.Partition
import collection.JavaConversions
import collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class MetaStoreSpec extends Specification {
    
     def YYYYMMDD( dtStr : String ) :DateTime = 
        MetaStore.YYYYMMDD.parseDateTime( dtStr) 
     

    "getIntervals" should {
        "getSpecified intervals" in {
            
            val randomDates =Seq(YYYYMMDD("20131015"), 
                   YYYYMMDD("20130801"),
                   YYYYMMDD("20130911"),
                   YYYYMMDD("20110101"),
                   YYYYMMDD("20120101"),
                   YYYYMMDD("20120911"),
                   YYYYMMDD("20130609"),
                   YYYYMMDD("20130703"),
                   YYYYMMDD("20130809")
                )
             val intervals = MetaStore.getIntervalsForDates( randomDates)
             intervals.foreach( interval =>{
                println(" Interval is " + interval.toString + " Start =  " + interval.getStart() + " to  " + interval.getEnd())
               } )

               true
        }
        "get Buckets of partitions " in {
        	val prodMetaStore = MetaStore( new java.net.URI("thrift://jobs-dev-sched2:9083"))
        	val dtNow = DateTime.now.toDateMidnight.toDateTime
        	val dtSeq = Seq( dtNow, dtNow.minusDays(3), dtNow.minusDays(7), dtNow.minusDays(30))
        	
        	val bucketedTables = prodMetaStore.getTablesByActivity(  "bi_maxwell", dtSeq)
        	
            bucketedTables.foreach { case( per, tableSeq) => {
                println(" For period " + per) 
                tableSeq.foreach( ethr => {
                   ethr match {
                     case tbl : Table =>
                        println("   Table " + tbl.getTableName()  +  " Created on " + tbl.getCreateTime ) 
                     case part : Partition => {
                        println("   Table Partition " + part.getTableName() + "( " + part.getParameters.entrySet.mkString(",") + " ) ") 
                     }
                   } 
                })
            } }
        	
        }
        
        /**
        "Clean up maxwell" in {
        	val prodMetaStore = MetaStore( new java.net.URI("thrift://jobs-dev-sched2:9083"))
        	prodMetaStore.cleanPartitionsForDb("bi_maxwell")
        	
            true 
        } 
        * **
        */
    }

}