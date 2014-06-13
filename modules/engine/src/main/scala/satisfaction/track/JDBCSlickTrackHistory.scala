package com.klout
package satisfaction
package track

import org.joda.time._
import engine.actors.GoalStatus
import engine.actors.GoalState
import slick.driver.H2Driver.simple._
import slick.jdbc.{GetResult, StaticQuery => Q}
import slick.jdbc.JdbcBackend.Database
import slick.lifted.ProvenShape
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import slick.jdbc.meta.MTable
import java.sql.Timestamp
import java.util.Date



case class DriverInfo(
	  val jdbcDriver : String =  "org.h2.Driver",
	  val dbURI : String = "jdbc:h2:file:data/sample", //change this to a file url, for persistence!
	  val user : String = "sa",
	  val passwd : String = "",
	  val mainTable : String = "TrackHistoryTable" 
);
	    

class JDBCSlickTrackHistory( driverInfo : DriverInfo ) extends TrackHistory{
	
	class TrackHistoryTable (tag: Tag) extends Table[(Int, String, String, String, String, String, String, Timestamp, Timestamp, String)](tag, "TrackHistoryTable") { // autoincrement is broken. I can't have id.? ~ for some reason...
  		  def id : Column[Int]= column[Int]("id", O.PrimaryKey, O.AutoInc)
		  def trackName : Column[String] = column[String]("trackName")
		  def forUser: Column[String] = column[String]("forUser")
		  def version: Column[String] = column[String]("version")
		  def variant: Column[String] = column[String]("variant")
		  def goalName: Column[String] = column[String]("goalName")
		  def witness: Column[String] = column[String]("witness")
		  def startTime: Column[Timestamp] = column[Timestamp]("startTime")
		  def endTime: Column[Timestamp] = column[Timestamp]("endTime")
		  def state: Column[String] = column[String]("state")
		  
		  def * : ProvenShape[(Int, String, String, String, String, String, String, Timestamp, Timestamp, String)] = (id, trackName, forUser, version, variant, goalName, witness, startTime, endTime, state)
		}
 //encapsulate driver information
	    
	  val table : TableQuery[TrackHistoryTable] = TableQuery[TrackHistoryTable]
	  val db = Database.forURL(driverInfo.dbURI, driverInfo.jdbcDriver) 
	  db withSession {
	    implicit Session =>
	      if (MTable.getTables(driverInfo.mainTable).list().isEmpty) {
	    	 table.ddl.create
	      }
	      
	  }
	
	

  //ex..startRun : create GoalRun obj from param, then insert into H2
	override def startRun(trackDesc : TrackDescriptor, goalName: String, witness: Witness, startTime: DateTime) : String =   {
	  db withSession {
	   implicit session =>
	     val insertResult: Option[Int] = table ++= Seq ( // rewite without Seq!
			 (1, trackDesc.trackName, trackDesc.forUser, trackDesc.version, trackDesc.variant.toString(), goalName, "dummyWitness", new Timestamp(startTime.getMillis()), new Timestamp(startTime.getMillis()), GoalState.Running.toString()) // FIX ENDTIME!!!
			)
		insertResult foreach {
		   numRows=> println(s"inserts $numRows into the table")
		 }
	 }
	 
	  "cheese" // need to return runID, which should be the id. But autoinc is broken right now
	}
	
	override def completeRun( id : String, state : GoalState.State) : String = {
	// update not a drop~~
	  
	  //might want to check the endDate timeStamp....
	  db withSession {
	   implicit session =>
	     val date : Date = new Date()
	     
	    Q.updateNA("UPDATE \"TrackHistoryTable\" SET endTime="+new Timestamp(date.getTime())+",state="+state.toString()+" WHERE id=" + id+";")
	  }
	  "Cheese" // what should we return? Probably the RunID; but that's broken right now
	}
	
	override def goalRunsForTrack(  trackDesc : TrackDescriptor , 
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun] = {
	  null
	}
	
	override  def goalRunsForGoal(  trackDesc : TrackDescriptor ,  
              goalName : String,
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun] = {
	  null
	}
	
	override def lookupGoalRun(  trackDesc : TrackDescriptor ,  
              goalName : String,
              witness : Witness ) : Seq[GoalRun] = {
			println("entering lookupGoalRun, " + trackDesc.trackName + " "+ trackDesc.forUser+ " "+ trackDesc.version+ " "+ trackDesc.variant+ " "+ goalName+ " "+ dummyWitnessToString(witness))
		 var returnList : Seq[GoalRun] = null.asInstanceOf[Seq[GoalRun]]
		db.withSession {
		   implicit session =>
		      table.list.map(e => println(" this is an entry: " + e._1 + " " + e._2 + " "+ e._3 + " " + e._4 + " " + e._5 + " " + e._6 + " " + e._7 + " " + e._8 + " " + e._9 + " " + e._10))
		      table.list.filter(g => (g._2 == trackDesc.trackName && // probably want filter then list for efficiency. Investigate whether type conversion in Table.Column == sting actually works
		         										g._3 == trackDesc.forUser &&
		         										 	g._4 == trackDesc.version &&
		         										 	//g._5 == trackDesc.variant && Variant is broken even though they both match "None" == "None" - must investigate
		         										 	g._6 == goalName &&
		         										 	g._7 == dummyWitnessToString(witness)	 
		    		 									)).map(g => println("  found a match! " + g._1 + g._2)
															       )
		      returnList = table.list.filter(g => (g._2 == trackDesc.trackName && // probably want filter then list for efficiency. Investigate whether type conversion in Table.Column == sting actually works
		         										 	g._3 == trackDesc.forUser &&
		         										 	g._4 == trackDesc.version &&
		         										 	g._5 == trackDesc.variant &&
		         										 	g._6 == goalName &&
		         										 	g._7 == dummyWitnessToString(witness)
		    		 									)).map(g => GoalRun(TrackDescriptor(g._2, g._3, g._4, Some(g._5)), 
															       	    g._6, dummyStringToWitness(g._7), new DateTime(g._8), 
															       	    Some(new DateTime(g._9)), GoalState(g._10.toInt))
															       ).seq
		     
		 }
			
	println("   the returned set size is: " + returnList.size)
	 returnList
	}
	
	def lookupGoalRun( runID : String ) : Option[GoalRun] = { 
	  var returnGoal : GoalRun = null.asInstanceOf[GoalRun]
	  db withSession {
	   implicit session =>
	     val g = table.filter(_.id === runID.toInt).list
	   	
	     val trackDesc :TrackDescriptor = TrackDescriptor(g(0)._2, g(0)._3, g(0)._4, Some(g(0)._5))
	     
	     val dtStart : DateTime = new DateTime(g(0)._8)
	     val dtEnd: Option[DateTime] = Some(new DateTime(g(0)._9))
	     returnGoal = GoalRun(trackDesc, g(0)._6, dummyStringToWitness(g(0)._7), dtStart, dtEnd, GoalState.WaitingOnDependencies)
	     //println("my resulting trackName is:" + returnGoal.trackDescriptor.trackName)
		}
	  Some(returnGoal)
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

/**
 *  
 */
object JDBCSlickTrackHistory extends JDBCSlickTrackHistory(new DriverInfo ) {
  
}