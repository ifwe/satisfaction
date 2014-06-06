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



