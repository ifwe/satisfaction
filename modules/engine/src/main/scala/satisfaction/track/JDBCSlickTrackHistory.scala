package com.klout
package satisfaction
package track

import org.joda.time._
import engine.actors.GoalStatus
import engine.actors.GoalState

import scala.slick.driver.H2Driver.simple._
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import scala.slick.jdbc.JdbcBackend.Database
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import Q.interpolation




//WE ARE USING H2!!!!!!@!!!!!!!!
// look up hibernate 

//encapsulate driver information
object H2DriverInfo {
  val JDBC_DRIVER : String =  "org.h2.Driver"
  val DB_URL : String = "jdbc:h2:mem:engine" //change this to a file url, for persistence!
  val USER : String = "sa"
  val PASS : String = ""
  val mainTable : String = "TrackHistoryTable"
    
    
  var conn : Connection = null  
  val db = Database.forURL(DB_URL, driver = JDBC_DRIVER) 
  db withSession  {
    implicit session => 
	      Q.updateNA("CREATE TABLE IF NOT EXISTS "+mainTable+" ("+
	    		  		"id int not null primary key AUTO_INCREMENT, " +
	    		  		"trackName varchar not null, " + 
	    		  		"forUser varchar, " + 
	    		  		"version varchar, " +
	    		  		"variant varchar, " +
	    		  		"goalName varchar not null, " +
	    		  		"witness varchar, " + 
	    		  		"startTime timestamp not null, " +
	    		  		"endTime timestamp, "+
	    		  		"state int" +
	    		  	")").execute
  }
}


class JDBCSlickTrackHistory extends TrackHistory{

  //ex..startRun : create GoalRun obj from param, then insert into H2
	override def startRun(trackDesc : TrackDescriptor, goalName: String, witness: Witness, startTime: DateTime) : String = H2DriverInfo.db.withSession {
	  implicit session =>
	     val goalRun : GoalRun = new GoalRun(trackDesc,
	    		 				goalName,
			  					witness, // this should be List[Witness] - when there are multiple witness variables
			  					startTime,
			  					null,
			  					GoalState.Running)
	     


    	Q.query[String, String]("INSERT INTO "+ H2DriverInfo.mainTable + " VALUES(" +
    	    "'"+trackDesc.trackName+"', "+
    	    "'"+trackDesc.forUser+"', "+
    	    "'"+trackDesc.version+"', "+
    	    "'"+trackDesc.variant+"', "+
    	    "'"+goalName+"', "+
    	    "'"+dummyWitnessToString(witness)+"', "+
    	    "'"+startTime.toString()+"', "+ // THIS IS PROBABLY WRONG! 	
    	    "null, "+
    	    "'"+GoalState.Running+"', "+
    	    ")")
	     
	     
	     goalRun.runId = 1.toString()
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
