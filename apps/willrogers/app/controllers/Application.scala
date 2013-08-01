package controllers

import play.api._
import hive.ms.MetaStore
import play.api.data._
import play.api.data.Forms._

import play.api.mvc._
import com.klout.satisfaction.executor.api._
import com.klout.satisfaction.common.dsl._

object Application extends Controller {

    def index = Action {
        Ok(views.html.toplevel())
    }

    def editHive = Action {
        Ok(views.html.hiveeditor())
    }

    def allDBs = Action {
        ///Ok( views.html.dbs( MetaStore.getDbs ) )
        Ok(views.html.dbtabs(MetaStore.getDbs))
    }

    def allProjects = Action {
        ///Ok( views.html.dbs( MetaStore.getDbs ) )
        ///Ok( views.html.dbtabs( MetaStore.getDbs ) )
        ///Ok(views.html.projtabs(List("Maxwell", "Topic Thunder", "Insights", "Relevance")))
        ///AsyncResult {
        ///Api.getProjects() map { projects =>
        val projects = SyncApi.getProjects
        val projNames: Set[String] = projects.names
        Ok(views.html.projtabs(projNames.toList))
    }

    def showProject(projName: String) = Action {
        val project = SyncApi.getProject(projName)
        val internalGoalList: Set[InternalGoal] = project.project.get.goals.filter(g =>
            g.isInstanceOf[InternalGoal]
        ).asInstanceOf[Set[InternalGoal]]

        val goalNameList = internalGoalList.map(_.name).toList
        val externalDepList = getExternalDependencies(internalGoalList)
        val dataOutputNames = externalDepList.map(displayDataOutput(_))
        Ok(views.html.showproject(projName, goalNameList, dataOutputNames))

    }

    def displayDataOutput(data: DataOutput): String = {
        data match {
            case tbl: HiveTable => "Table " + tbl.name
            case pth: HdfsPath  => "Path " + pth.path
            case _              => data.toString()
        }
    }

    def getExternalDependencies(internalGoalList: Set[InternalGoal]): List[DataOutput] = {
        var depList = List[DataOutput]()
        println(" GoalList = " + internalGoalList)
        for (g <- internalGoalList) {
            if (g.externalDependsOn != null)
                depList = depList ::: g.externalDependsOn.map(_.dependsOn).flatten.toList
            if (g.dependsOn != null)
                depList = depList ::: getExternalDependencies(g.dependsOn)
        }
        println(" Dep List is " + depList)
        depList.toList.distinct
    }

    def getDBTables(db: String) = Action {
        Ok(views.html.alltables(db, MetaStore.getTables(db)))
    }

    def showDBTable(db: String, tblName: String) = Action {
        val tbl = MetaStore.getTableByName(db, tblName)
        val partNames = MetaStore.getPartitionNamesForTable(db, tblName)
        Ok(views.html.showtable(db, tblName, tbl, partNames))
    }

    def showPartition(db: String, tblName: String, partName: String) = Action {
        val tbl = MetaStore.getTableByName(db, tblName)
        val part = MetaStore.getPartitionByName(db, tblName, partName.replace('@', '/'))
        Ok(views.html.showpartition(db, tblName, part))
    }

}