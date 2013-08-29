package controllers

import play.api._
import hive.ms.MetaStore
import play.api.data._
import play.api.data.Forms._

import play.api.mvc._
import com.klout.satisfaction._
import com.klout.satisfaction.executor.api._

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
        ///Ok(views.html.projtabs(List("Maxwell", "Topic Thunder", "Insights", "Relevance")))
        ///val projects = SyncApi.getProjects
        ///val projNames: Set[String] = projects.names

        val projNames = Set("Maxwell", "Topic Thunder", "Insights", "Bing")
        Ok(views.html.projtabs(projNames.toList))
    }

    def displayDataOutput(data: DataOutput): String = {
        data match {
            case tbl: HiveTable => "Table " + tbl.tblName
            case pth: HdfsPath  => "Path " + pth.path
            case _              => data.toString()
        }
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