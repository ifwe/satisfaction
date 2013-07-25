package controllers

import play.api._
import play.api.mvc._
import Results._

import com.klout.satisfaction.common.dsl._
import org.joda.time._

import scala.collection.JavaConversions._
import java.net._

object jsonable {
    import play.api.libs.json._

    def json[T: Writes](t: T) = Ok(Json toJson t)
}

object Samples extends Controller {

    import jsonable._

    def showProject(jarName: String, className: String) = Action {
        val url = new URL(s"file:///private/tmp/${jarName}")
        val urls = Array(url)
        val ucl = new URLClassLoader(urls, classOf[ProjectProvider].getClassLoader)
        val clazz = ucl.loadClass(className)
        val provider = clazz.getField("MODULE$").get(clazz).asInstanceOf[ProjectProvider]
        val project = provider.project
        json(Map("project" -> project.toString))
    }

}
