package controllers

import play.mvc.Controller
import hive.ms.SLA;
import play.api.data.Form
import play.api.data.Forms._

/**
 *
 */
object SlaPage extends Controller {

    val slaForm: Form[SLA] = null
    //Form(
    ///mapping(
    //)
    //)

    def displaySla(dbName: String, tblName: String) = {

    }

}