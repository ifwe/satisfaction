package controllers

import play.api._
import play.api.data._
import play.api.data.Forms._
import play.mvc.Controller
import play.api.mvc.Results._
import play.api.data.validation.Constraints._
import play.api.mvc.Action
import models.MetaDataHolder
import hive.ms._
import play.api.mvc.Call

object MetaDataPage extends Controller {
    val metadataForm: Form[MetaDataHolder] = Form(
        mapping("key" -> nonEmptyText,
            "value" -> text
        ) (MetaDataHolder.apply)(MetaDataHolder.unapply)
    )

    def addMetaData(db: String, tblName: String) = Action { implicit request =>
        metadataForm.bindFromRequest.fold(
            formWithErrors => Status(500),
            metadataHolder => {

                MetaStore.setTableMetaData(db, tblName, metadataHolder.key, metadataHolder.value)
                Ok(views.html.metadata(db, tblName, metadataForm))
            }
        )
    }

    def showMetaData(db: String, tblName: String) = Action {
        Ok(views.html.metadata(db, tblName, metadataForm))
    }

}