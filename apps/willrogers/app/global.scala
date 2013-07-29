import play.api._

import com.klout.satisfaction.executor.api._

object Global extends GlobalSettings {

    val HdfsProjectRootPath = "/satisfaction/prod/projects"

    override def onStart(app: Application) {
        Api.initProjects(HdfsProjectRootPath)
    }
}