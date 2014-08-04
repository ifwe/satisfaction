package willrogers
package controllers

import play.api._
import play.api.data._
import play.api.data.Forms._
import play.mvc.Controller
import play.api.mvc.Results._
import play.api.data.validation.Constraints._
import play.api.mvc.Action
import models.MetaDataHolder
import satisfaction.hadoop.hive.ms._
import play.api.mvc.Call

object MetaDataPage extends Controller {
    val ms : MetaStore = Global.metaStore
    
    val metadataForm: Form[MetaDataHolder] = Form(
        mapping("key" -> nonEmptyText,
            "value" -> text
        ) (MetaDataHolder.apply)(MetaDataHolder.unapply)
    )

    def addMetaData(db: String, tblName: String) = Action { implicit request =>
        println(" Adding Meta Data ")
        metadataForm.bindFromRequest.fold(
            formWithErrors => Status(500),
            metadataHolder => {

                println(" Adding Meta Data in holder  " + metadataHolder)
                ms.setTableMetaData(db, tblName, metadataHolder.key, metadataHolder.value)
                Ok(views.html.metadata(db, tblName, ms, metadataForm))
            }
        )
    }

    def showMetaData(db: String, tblName: String) = Action {
        Ok(views.html.metadata(db, tblName, ms, metadataForm))
    }

}