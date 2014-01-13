package com.klout
package controllers

import play.api._
import satisfaction.hadoop.hive.ms.MetaStore
import satisfaction.hadoop.hive.ms._
import satisfaction.hadoop.hdfs._
import play.api.data._
import play.api.data.Forms._

import play.api.mvc._
import com.klout.satisfaction._

object Application extends Controller {

    def index = Action {
        Ok(views.html.toplevel())
    }


    def allDBs = Action {
        ///Ok( views.html.dbs( MetaStore.getDbs ) )
        Ok(views.html.dbtabs(MetaStore.getDbs))
    }


    def displayDataOutput(data: DataOutput): String = {
        data match {
            case tbl: HiveTable => "Table " + tbl.tblName
            case pth: HdfsPath  => "Path " + pth.path
            case _              => data.toString()
        }
    }

    def getDBTables(db: String) = Action {
        Ok(views.html.alltables(db, MetaStore.getTables(db), MetaStore.getViews(db)))
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