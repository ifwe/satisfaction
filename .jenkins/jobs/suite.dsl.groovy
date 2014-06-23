@GrabResolver('https://artifactory.tagged.com/artifactory/libs-release-local/')
@Grab('com.tagged.build:jenkins-dsl-common:0.1.20')

import com.tagged.build.common.*

def satisfaction = new Project(
    jobFactory,
    [
        githubOwner: 'jbanks',
        githubProject: 'satisfaction-limbo',
        hipchatRoom: 'Cluster Corner',
        email: 'jbanks@tagged.com',
    ]
).basicJob {
    description "Satisfaction Scheduler"
    label "scala"
    jdk "jdk 7u51"
    steps {
        sbt('sbt',
            'clean publish',
            '-Dsbt.log.noformat=true -Dsbt.override.build.repos=true',
            '-Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m')
        sbt('sbt',
            '\'project willrogers\' rpm:packageBin',
            '-Dsbt.log.noformat=true -Dsbt.override.build.repos=true',
            '-Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m')
    }
    triggers {
        githubPush()
    }
}
