sbt-satisfy
-----------

SBT plugin for deploying Satisfaction projects

Include this plugin to deploy your Satisfaction tracks
 to the correct place on HDFS to be scheduled 


To add the sbt-satisfy plugin to your project, 
 perform the following steps 

* Add the following lines to your project/plugins.sbt file 

```
addSbtPlugin("com.tagged.satisfaction" % "sbt-satisfy" % "0.1") 
```
   
* Import the sbt-satisfy keys in your build.sbt

```
import sbtSatisfy._
import SatisfyKeys._
```


* Define your track name as property in build.sbt

```
trackName := "MyAwesomeTrack"
```

*  When you are ready to upload to the Satisfaction Scheduler,
  run the `upload` command from the sbt prompt.

```
sbt
> upload
```

* This will upload the project into an HDFS directory where Satisfaction 
   expects it to be, under /user/satisfaction/track

```
> hadoop dfs -ls /user/satisfaction/track/MyAwesomeTrack
Found 1 items
drwxr-x--x   - jbanks hadoop          0 2014-06-10 15:20 /user/satisfaction/track/MyAwesomeTrack/version_0.1
```

*  Define a Track using the Satisfaction Scala DSL under src/main/scala/ .  

*  If you have resources to be uploaded ( like some awesome Hive scripts ),
     place them under src/main/resources

*  Create a file in the conf/ directoy, named "satisfaction.properties".  This contains any sort of configuration properties
   which need to be specified which aren't the Track definition, ( defined in the Scala DSL ) or a resource file

*   Add a property 'satisfaction.track.class' which is the class name of the Scala DSL Track definition


