@GrabResolver('https://artifactory.tagged.com/artifactory/libs-release-local/')
@Grab('com.tagged.build:jenkins-dsl-common:[0.1.0,)')

import com.tagged.build.scm.*
import com.tagged.build.common.*

def satisfaction = new Project(
    jobFactory,
    [
        scm: new StashSCM(project: "hadoop", name: "satisfaction-limbo"),
        hipchatRoom: 'Cluster Corner',
        email: 'jbanks@tagged.com',
    ]
).basicJob {
    description "Satisfaction Scheduler"
    label "scala"
    jdk "jdk 7u51"
    steps {
        sbt('sbt',
            '\'project satisfaction-core\' clean publish',
            '-Dsbt.log.noformat=true',
            '-Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m')
        sbt('sbt',
            '\'project satisfaction-engine\' clean publish',
            '-Dsbt.log.noformat=true',
            '-Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m')
        sbt('sbt',
            '\'project satisfaction-hadoop\' clean publish',
            '-Dsbt.log.noformat=true',
            '-Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m')
        sbt('sbt',
            '\'project satisfaction-hive\' clean publish',
            '-Dsbt.log.noformat=true',
            '-Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m')
        sbt('sbt',
            '\'project willrogers\' rpm:packageBin',
            '-Dsbt.log.noformat=true',
            '-Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m')
    }
    publishers {
        archiveArtifacts('apps/willrogers/target/rpm/RPMS/noarch/*.rpm')
    }
}
