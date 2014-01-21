package willrogers
package controllers

import play.api._

import com.klout.satisfaction.hadoop.hive.ms._
import com.klout.satisfaction.hadoop.hdfs._
import play.api.data._
import play.api.data.Forms._

import play.api.mvc._
import com.klout.satisfaction._


object Application extends Controller {
    val ms = Global.metaStore

    def index = Action {
        Ok(views.html.toplevel())
    }


    def allDBs = Action {
        ///Ok( views.html.dbs( MetaStore.getDbs ) )
        Ok(views.html.dbtabs(ms.getDbs))
    }


    def displayDataOutput(data: DataOutput): String = {
        data match {
            case tbl: HiveTable => "Table " + tbl.tblName
            case pth: HdfsPath  => "Path " + pth.path
            case _              => data.toString()
        }
    }

    def getDBTables(db: String) = Action {
        Ok(views.html.alltables(db, ms.getTables(db), ms.getViews(db)))
    }

    def showDBTable(db: String, tblName: String) = Action {
        val tbl = ms.getTableByName(db, tblName)
        val partNames = ms.getPartitionNamesForTable(db, tblName)
        Ok(views.html.showtable(db, tblName, ms, tbl, partNames))
    }

    def showPartition(db: String, tblName: String, partName: String) = Action {
        val tbl = ms.getTableByName(db, tblName)
        val part = ms.getPartitionByName(db, tblName, partName.replace('@', '/'))
        Ok(views.html.showpartition(db, tblName, ms, part))
    }

}