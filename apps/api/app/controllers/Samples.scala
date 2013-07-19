package controllers

import play.api._
import play.api.mvc._
import Results._

import com.klout.satisfaction.common.dsl._
import org.joda.time._

object jsonable {
    import play.api.libs.json._

    def json[T: Writes](t: T) = Ok(Json toJson t)
}

object Samples extends Controller {
    import jsonable._

    def getSampleProject() = Action {
        val paths = List(HiveTable("foo"), HdfsPath("bar"))
        val goalContext = GoalContext("foo", DateTime.now, Map("foo" -> "bar"))
        json(Map("hi" -> goalContext))
    }

}
