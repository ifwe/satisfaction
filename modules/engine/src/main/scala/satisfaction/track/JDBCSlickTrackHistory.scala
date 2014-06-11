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
	
 //case class TrackHistoryTable(val id: String, val trackName: String, val forUser:String, val version:String, val variant: String, val goalName: String, val witness: String, val startTime: Timestamp, val endTime: Timestamp, val state:String) {}
	class TrackHistoryTable (tag: Tag) extends Table[(Int, String, String, String, String, String, String, Timestamp, Timestamp, String)](tag, "TrackHistoryTable") { // autoincrement is broken. I can't have id.? ~ for some reason...
	//object trackEntry extends Table[(Int, String, String, String, String, String, String, Timestamp, Timestamp, String)]("TrackHistoryTable") {
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
	
	

  //ex..startRun : create GoalRun obj from param, then insert into H2
	override def startRun(trackDesc : TrackDescriptor, goalName: String, witness: Witness, startTime: DateTime) : String =   {
	 H2DriverInfo.db withSession {
	   implicit session =>
	     val insertResult: Option[Int] = this.H2DriverInfo.table ++= Seq ( // rewite without Seq!
			 (1, trackDesc.trackName, trackDesc.forUser, trackDesc.version, trackDesc.variant.toString(), goalName, "dummyWitness", new Timestamp(startTime.getMillis()), new Timestamp(startTime.getMillis()), GoalState.Running.toString()) // FIX ENDTIME!!!
			)
		insertResult foreach {
		   numRows=> println(s"inserts $numRows into the table")
		 }
	 }
	 
	  "cheese"
	}
	
	override def completeRun( id : String, state : GoalState.State) : String = {
	// update not a drop~~
	  
	  //might want to check the endDate timeStamp....
	  H2DriverInfo.db withSession {
	   implicit session =>
	     val date : Date = new Date()
	     
	    Q.updateNA("UPDATE \"TrackHistoryTable\" SET endTime="+new Timestamp(date.getTime())+",state="+state.toString()+" WHERE id=" + id+";")
	  }
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
	 var returnList : GoalRun = null.asInstanceOf[GoalRun]
	 /*
	  H2DriverInfo.db withSession {
	   implicit session =>
	     val g = this.H2DriverInfo.table.filter(l => (l.)).list
	   	
	     val trackDesc :TrackDescriptor = TrackDescriptor(g(0)._2, g(0)._3, g(0)._4, Some(g(0)._5))
	     returnGoal = GoalRun(trackDesc, g(0)._6, null, new DateTime(g(0)._8.toLocalDateTime()), null, GoalState.WaitingOnDependencies) // rewrite endtime and goalstate
	   }
	  Some(returnList)
	  null
	  * 
	  */
	}
	
	def lookupGoalRun( runID : String ) : Option[GoalRun] = { 
	  var returnGoal : GoalRun = null.asInstanceOf[GoalRun]
	  H2DriverInfo.db withSession {
	   implicit session =>
	     val g = this.H2DriverInfo.table.filter(_.id === runID.toInt).list
	   	
	     val trackDesc :TrackDescriptor = TrackDescriptor(g(0)._2, g(0)._3, g(0)._4, Some(g(0)._5))
	     returnGoal = GoalRun(trackDesc, g(0)._6, null, new DateTime(g(0)._8.toLocalDateTime()), null, GoalState.WaitingOnDependencies) // rewrite endtime and goalstate
	   }
	  Some(returnGoal)
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