package satisfaction
package track

import org.joda.time._
import GoalStatus._
import scala.slick.driver.H2Driver.simple._
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import scala.slick.jdbc.JdbcBackend.Database
import scala.slick.lifted.ProvenShape
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import scala.slick.jdbc.meta.MTable
import java.sql.Timestamp

import satisfaction.track.Witness2Json._



/**
 * Using slick with H2 as our light-weight db
 */

case class DriverInfo(
	  val jdbcDriver : String =  "org.h2.Driver",
	  val dbURI : String = "jdbc:h2:file:data/sample", //change this to a file url, for persistence!
	  val user : String = "sa",
	  val passwd : String = "",
	  val props : java.util.Properties = new java.util.Properties
);


class JDBCSlickTrackHistory( val driverInfo : DriverInfo)   extends TrackHistory{
	/**
	 * class for database formatting
	 */
  /**
    case class TrackHistoryTableType( val id :Int,
           val trackName:String,
           val forUser:String,
           val version:String,
           val variant:String,val goalName:String,
           val witness:String,val startTime:Timestamp, 
           val endTime:Option[Timestamp], String)
           * 
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
	
	  val table : TableQuery[TrackHistoryTable] = TableQuery[TrackHistoryTable]
	  
	  val mainTable : String = "TrackHistoryTable"
	  val db = Database.forURL(driverInfo.dbURI,
	          driver = driverInfo.jdbcDriver, 
	          user=driverInfo.user, 
	          password=driverInfo.passwd,
	          prop = driverInfo.props)
	  val tblCreate = db withSession {
	    implicit Session =>
	      if (MTable.getTables(mainTable).list().isEmpty) {
	    	 table.ddl.create
	      }
	  }



	override def startRun(trackDesc : TrackDescriptor, goalName: String, witness: Witness, startTime: DateTime) : String =   {
	  var insertedID = -1
	 db withSession {
	   implicit session =>
		insertedID = (table returning table.map(_.id)) += (1, trackDesc.trackName, trackDesc.forUser, trackDesc.version, trackDesc.variant.toString(), 
																					goalName, renderWitness(witness), new Timestamp(startTime.getMillis()), None, 
																					GoalState.Running.toString())
	 }
	  insertedID.toString
	}
	
	override def completeRun( id : String, state : GoalState.State) : String = {
	  db withSession {
	   implicit session =>
	     val check = table.filter( _.id === id.toInt ).
	       map( x => ( x.state , x.endTime)).update( (state.toString, Some(new Timestamp(DateTime.now.getMillis))))
	         
	  }
	  id 
	}
	
	override def goalRunsForTrack(  trackDesc : TrackDescriptor , 
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun] = {
	  var returnList : Seq[GoalRun] = null.asInstanceOf[Seq[GoalRun]]
	  db withSession {
		   implicit session =>
		     
		   	 returnList=table.list.filter(g=>(g._2 == trackDesc.trackName &&
		         								g._3 == trackDesc.forUser &&
		         								g._4 == trackDesc.version &&
		         								(g._5 match { case v if !(v == "None") => v == trackDesc.variant
		         										 	  case v if (v == "None") => !trackDesc.variant.isDefined}) &&
		         								(startTime match { case Some(dateTime) =>
		         								  						new DateTime(g._8).compareTo(dateTime.asInstanceOf[DateTime]) >= 0
										    		 			   case None => true
					    		 							}) &&
					    		 				(endTime match {case Some(dateTime) if g._9.isDefined =>
					    		 				  					new DateTime(g._9.get).compareTo(dateTime.asInstanceOf[DateTime]) <= 0
									    		 				case Some(dateTime) if !g._9.isDefined => false
									    		 				case None => true
					    		 							})
		   			 							)).map(g => {
		   			 							  val gr = GoalRun(TrackDescriptor(g._2, g._3, g._4, Some(g._5)), 
															       	    g._6, parseWitness(g._7), new DateTime(g._8), 
															       	    g._9 match { case Some(timestamp) => Some(new DateTime(timestamp))
															       	    			 case None => null}, GoalState.withName(g._10))
													 gr.runId = g._1.toString
													 gr
		   			 							}).seq
			}
	  returnList
	}
	
	override  def goalRunsForGoal(  trackDesc : TrackDescriptor ,  
              goalName : String,
              startTime : Option[DateTime], endTime : Option[DateTime] ) : Seq[GoalRun] = {
	  
	  var returnList : Seq[GoalRun] = null.asInstanceOf[Seq[GoalRun]]
	  db withSession {
		   implicit session =>
		   		   	 returnList= table.list.filter(g=>(g._2 == trackDesc.trackName &&
		         								g._3 == trackDesc.forUser &&
		         								g._4 == trackDesc.version &&
		         								(g._5 match {
		         										 	  case v if !(v == "None") => v == trackDesc.variant
		         										 	  case v if (v == "None") => !trackDesc.variant.isDefined}) &&
		         								g._6 == goalName &&
		         								(startTime match { case Some(dateTime) =>
		         								  						new DateTime(g._8).compareTo(dateTime.asInstanceOf[DateTime]) >= 0
										    		 			   case None => true
					    		 							}) &&
					    		 				(endTime match {case Some(dateTime) if g._9.isDefined =>
					    		 				  					new DateTime(g._9.get).compareTo(dateTime.asInstanceOf[DateTime]) <= 0
									    		 				case Some(dateTime) if !g._9.isDefined => false
									    		 				case None => true
					    		 							})
		   			 							)).map(g => {
		   			 							  val gr = GoalRun(TrackDescriptor(g._2, g._3, g._4, Some(g._5)), 
															       	    g._6, parseWitness(g._7), new DateTime(g._8), 
															       	    g._9 match { case Some(timestamp) => Some(new DateTime(timestamp))
															       	    			 case None => null}, GoalState.withName(g._10))
														gr.runId=g._1.toString
														gr
		   			 							}).seq
			}
	  returnList
	}	
	
	override def lookupGoalRun(  trackDesc : TrackDescriptor ,  
              goalName : String,
              witness : Witness ) : Seq[GoalRun] = {
		 var returnList : Seq[GoalRun] = null.asInstanceOf[Seq[GoalRun]]
		 db withSession {
		   implicit session =>
		     
		     returnList = table.list.filter(g => (g._2 == trackDesc.trackName && // probably want: filter then list for efficiency. But it breaks comparison
		         										 	g._3 == trackDesc.forUser &&
		         										 	g._4 == trackDesc.version &&
		         										 	(g._5 match {
		         										 	  case v if !(v == "None") => v == trackDesc.variant
		         										 	  case v if (v == "None") => !trackDesc.variant.isDefined}) &&
		         										 	g._6 == goalName &&
		         										 	g._7 == renderWitness(witness)
		    		 									)).map(g => {
		    		 									  val gr = GoalRun(TrackDescriptor(g._2, g._3, g._4, Some(g._5)), 
															       	    g._6, parseWitness(g._7), new DateTime(g._8), 
															       	    g._9 match { case Some(timestamp) => Some(new DateTime(timestamp)) case None => null}, GoalState.withName(g._10)) 
															       	    gr.runId = g._1.toString
															       	    gr
		    		 											}).seq
		 }
		returnList
	}
	
	def lookupGoalRun( runID : String ) : Option[GoalRun] = { 
	  var returnGoal : GoalRun = null.asInstanceOf[GoalRun]
	  db withSession {
	   implicit session =>
	     val g = table.filter(_.id === runID.toInt).list
	   	
	     if (!g.isEmpty) {
	    	 val trackDesc :TrackDescriptor = TrackDescriptor(g(0)._2, g(0)._3, g(0)._4, Some(g(0)._5))
	     
		     val dtStart : DateTime = new DateTime(g(0)._8)
		     val dtEnd = g(0)._9 match { 
		       case Some(timestamp) => Some(new DateTime(timestamp))
		       case None => None
		     }
		     returnGoal = GoalRun(trackDesc, g(0)._6, parseWitness(g(0)._7), dtStart, dtEnd, GoalState.withName(g(0)_10))
		     returnGoal.runId = g(0)._1.toString
		     Some(returnGoal)
	     } else {
	       None
	     }
		}
	}

	
	
	def getAllHistory() : Seq[GoalRun] = {
	  // println("grabbing all tracks") takes about 2 seconds :/
	  var returnList : Seq[GoalRun] = null.asInstanceOf[Seq[GoalRun]]
	  db.withSession {
		   implicit session =>
		   		   	 returnList=table.list.map(g => {
		   			 							  val gr = GoalRun(TrackDescriptor(g._2, g._3, g._4, Some(g._5)), 
															       	    g._6, parseWitness(g._7), new DateTime(g._8), 
															       	    g._9 match { case Some(timestamp) => Some(new DateTime(timestamp))
															       	    			 case None => null}, GoalState.withName(g._10))
														gr.runId=g._1.toString
														gr
		   			 							}).seq
			}
	  //println("I got all the tracks!")
	  returnList
	}
	

  
}

object JDBCSlickTrackHistory extends JDBCSlickTrackHistory( new DriverInfo) {

}