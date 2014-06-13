package com.klout
package satisfaction
package track

import org.joda.time._
import engine.actors.GoalStatus
import engine.actors.GoalState
import scala.slick.driver.H2Driver.simple._
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import scala.slick.jdbc.JdbcBackend.Database
import scala.slick.lifted.ProvenShape
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import Q.interpolation
import scala.slick.jdbc.meta.MTable
import java.sql.Timestamp
import java.util.Date




//WE ARE USING H2!!!!!!@!!!!!!!!
// look up hibernate 


class JDBCSlickTrackHistory extends TrackHistory{
	/**
	 * class for database formatting
	 */
	class TrackHistoryTable (tag: Tag) extends Table[(Int, String, String, String, String, String, String, Timestamp, Option[Timestamp], String)](tag, "TrackHistoryTable") {
  		  def id : Column[Int]= column[Int]("id", O.PrimaryKey, O.AutoInc)
		  def trackName : Column[String] = column[String]("trackName")
		  def forUser: Column[String] = column[String]("forUser")
		  def version: Column[String] = column[String]("version")
		  def variant: Column[String] = column[String]("variant")
		  def goalName: Column[String] = column[String]("goalName")
		  def witness: Column[String] = column[String]("witness")
		  def startTime: Column[Timestamp] = column[Timestamp]("startTime")
		  def endTime: Column[Option[Timestamp]] = column[Option[Timestamp]]("endTime", O.Nullable)
		  def state: Column[String] = column[String]("state")
		  
		  def * : ProvenShape[(Int, String, String, String, String, String, String, Timestamp, Option[Timestamp], String)] = (id, trackName, forUser, version, variant, goalName, witness, startTime, endTime, state)
		}
	
	/**
	 * Encapsulate DB drivers/info
	 */
	object H2DriverInfo {
	  val JDBC_DRIVER : String =  "org.h2.Driver"
	  val DB_URL : String = "jdbc:h2:file:data/sample" //change this to a file url, for persistence!
	  val USER : String = "sa"
	  val PASS : String = ""
	  val mainTable : String = "TrackHistoryTable"
	    
	  val table : TableQuery[TrackHistoryTable] = TableQuery[TrackHistoryTable]
	  var db = Database.forURL(DB_URL, driver = JDBC_DRIVER) 
	  db withSession {
	    implicit Session =>
	      if (MTable.getTables("TrackHistoryTable").list().isEmpty) {
	    	 table.ddl.create
	      }
	  }
	} // object H2Driverinfo
	
	override def startRun(trackDesc : TrackDescriptor, goalName: String, witness: Witness, startTime: DateTime) : String =   {
	  var insertedID = -1
	 H2DriverInfo.db withSession {
	   implicit session =>
		insertedID = (H2DriverInfo.table returning H2DriverInfo.table.map(_.id)) += (1, trackDesc.trackName, trackDesc.forUser, trackDesc.version, trackDesc.variant.toString(), 
																					goalName, "dummyWitness", new Timestamp(startTime.getMillis()), None, 
																					GoalState.Running.toString())
	 }
	  insertedID.toString
	}
	
	override def completeRun( id : String, state : GoalState.State) : String = {
	  H2DriverInfo.db withSession {
	   implicit session =>
	     val date : Date = new Date()
	     
	    val updateEndTime = for {e <- H2DriverInfo.table if e.id === id.toInt} yield e.endTime 
	    updateEndTime.update(Some(new Timestamp (date.getTime())))
	    
	    val updateState = for {e <-H2DriverInfo.table if e.id === id.toInt} yield e.state
	    updateState.update(state.toString())
	    
	  }
	  id // what should we return? Probably the RunID??
	}
	
	override def goalRunsForTrack(  trackDesc : TrackDescriptor , 
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun] = {
	  null
	}
	
	override  def goalRunsForGoal(  trackDesc : TrackDescriptor ,  
              goalName : String,
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun] = {
	  var returnList : Seq[GoalRun] = null.asInstanceOf[Seq[GoalRun]]
	  H2DriverInfo.db.withSession {
		   implicit session =>
		     H2DriverInfo.table.list.map(e => println(" this is an entry: " + e._1 + " " + e._2 + " "+ e._3 + " " + e._4 + " " + e._5 + " " + e._6 + " " + e._7 + " " + e._8 + " " + e._9 + " " + e._10))

		   	 returnList=H2DriverInfo.table.list.filter(g=>(g._2 == trackDesc.trackName &&
		         								g._3 == trackDesc.forUser &&
		         								g._4 == trackDesc.version &&
		         								(g._5 match {
		         										 	  case v if !(v == "None") => v == trackDesc.variant
		         										 	  case v if (v == "None") => !trackDesc.variant.isDefined}) &&
		         								
		         								g._6 == goalName )).filter(g=> (startTime match { // I don't like this double filtering. Need to figure out syntax for an elegant solution.
										    		 							  case Some(dateTime) =>
										    		 							    new DateTime(g._8).compareTo(startTime.asInstanceOf[DateTime]) >= 0
										    		 							  case None => true
					    		 							})).filter(g=> (endTime match {
									    		 							  case Some(dateTime) if g._9.isDefined =>
									    		 							    new DateTime(g._9).compareTo(endTime.asInstanceOf[DateTime]) <= 0
									    		 							  case Some(dateTime) if !g._9.isDefined => false
									    		 							  case None => true
					    		 							})).map(g => GoalRun(TrackDescriptor(g._2, g._3, g._4, Some(g._5)), 
															       	    g._6, dummyStringToWitness(g._7), new DateTime(g._8), 
															       	    g._9 match { case Some(timestamp) => Some(new DateTime(timestamp)) case None => null}, GoalState.withName(g._10))).seq
			}
	  returnList
	}	
	
	override def lookupGoalRun(  trackDesc : TrackDescriptor ,  
              goalName : String,
              witness : Witness ) : Seq[GoalRun] = {
			//println("entering lookupGoalRun, " + trackDesc.trackName + " "+ trackDesc.forUser+ " "+ trackDesc.version+ " "+ trackDesc.variant+ " "+ goalName+ " "+ dummyWitnessToString(witness))
		 var returnList : Seq[GoalRun] = null.asInstanceOf[Seq[GoalRun]]
		 H2DriverInfo.db.withSession {
		   implicit session =>
		     //H2DriverInfo.table.list.map(e => println(" this is an entry: " + e._1 + " " + e._2 + " "+ e._3 + " " + e._4 + " " + e._5 + " " + e._6 + " " + e._7 + " " + e._8 + " " + e._9 + " " + e._10))
		     
		     returnList = H2DriverInfo.table.list.filter(g => (g._2 == trackDesc.trackName && // probably want: filter then list for efficiency. But it breaks comparison
		         										 	g._3 == trackDesc.forUser &&
		         										 	g._4 == trackDesc.version &&
		         										 	(g._5 match {
		         										 	  case v if !(v == "None") => v == trackDesc.variant
		         										 	  case v if (v == "None") => !trackDesc.variant.isDefined}) &&
		         										 	g._6 == goalName &&
		         										 	g._7 == dummyWitnessToString(witness)
		    		 									)).map(g => GoalRun(TrackDescriptor(g._2, g._3, g._4, Some(g._5)), 
															       	    g._6, dummyStringToWitness(g._7), new DateTime(g._8), 
															       	    g._9 match { case Some(timestamp) => Some(new DateTime(timestamp)) case None => null}, GoalState.withName(g._10))
		    		 											).seq // TODO: STICK RUNID IN HERE!!!! HOW?!?!??!?!?
		   //println("  lookingGoalRun result set is size: " + returnList.size)
		 }	
		returnList
	}
	
	def lookupGoalRun( runID : String ) : Option[GoalRun] = { 
	  var returnGoal : GoalRun = null.asInstanceOf[GoalRun]
	  H2DriverInfo.db withSession {
	   implicit session =>
	     val g = this.H2DriverInfo.table.filter(_.id === runID.toInt).list
	   	
	     if (!g.isEmpty) {
	    	 val trackDesc :TrackDescriptor = TrackDescriptor(g(0)._2, g(0)._3, g(0)._4, Some(g(0)._5))
	     
		     val dtStart : DateTime = new DateTime(g(0)._8)
		     val dtEnd = g(0)._9 match {
		       case Some(timestamp) => Some(new DateTime(timestamp))
		       case None => None
		     }
		     returnGoal = GoalRun(trackDesc, g(0)._6, dummyStringToWitness(g(0)._7), dtStart, dtEnd, GoalState.WaitingOnDependencies)
		     returnGoal.runId = g(0)._1.toString
		     Some(returnGoal)
	     } else {
	       None
	     }
		}
	}
	
	//dummy method - wait for Jerome
	def dummyWitnessToString ( witness : Witness) : String = {
	  "dummyWitness"
	}
	
	def dummyStringToWitness(string : String ) : Witness = {
	   null
	} 
	
	
	/**
	 * Insert a GoalRun object into the db
	 
	def insert (goalRun: GoalRun) : String  = { //generate a runID
	  
	}
	*/
  
}