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




//WE ARE USING H2!!!!!!@!!!!!!!!
// look up hibernate 


class JDBCSlickTrackHistory extends TrackHistory{

 //encapsulate driver information
	object H2DriverInfo {
	  
	  class TrackHistoryTable (tag: Tag) extends Table[(Int, String)](tag, "TrackHistoryTable") {
		  def id : Column[Int]= column[Int]("id", O.PrimaryKey)
		  def trackName : Column[String] = column[String]("trackName")
		  
		  def * : ProvenShape[(Int, String)] = (id, trackName)
		}
	  
	  
	  val JDBC_DRIVER : String =  "org.h2.Driver"
	  val DB_URL : String = "jdbc:h2:mem:engine" //change this to a file url, for persistence!
	  val USER : String = "sa"
	  val PASS : String = ""
	  val mainTable : String = "TrackHistoryTable"
	    
	  val table : TableQuery[TrackHistoryTable] = TableQuery[TrackHistoryTable]
	  var db = Database.forURL(DB_URL, driver = JDBC_DRIVER) 
	  db withSession {
	    implicit Session =>
	      table.ddl.create
	      val insertResult: Option[Int] = table ++= Seq (
			 (1, "track1"),
			 (2, "track2")
			)

		insertResult foreach {
		   numRows=> println(s"inserts $numRows into the table")
		 }
	  }
	}
	
	

  //ex..startRun : create GoalRun obj from param, then insert into H2
	override def startRun(trackDesc : TrackDescriptor, goalName: String, witness: Witness, startTime: DateTime) : String =   {
	 H2DriverInfo.db withSession {
	   implicit session =>
	     val insertResult: Option[Int] = this.H2DriverInfo.table ++= Seq (
			 (3, "track3"),
			 (4, "track4")
			)
		insertResult foreach {
		   numRows=> println(s"inserts $numRows into the table")
		 }
	 }
	 
	  "cheese"
	}
	
	override def completeRun( id : String, state : GoalState.State) : String = {
	// update not a drop~~
	  "Cheese"
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
	  null
	}
	
	def lookupGoalRun( runID : String ) : Option[GoalRun] = { 
	  null
	}
	
	//dummy method
	def dummyWitnessToString ( witness : Witness) : String = {
	  "cheese"
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