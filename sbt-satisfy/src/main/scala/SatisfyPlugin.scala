package sbtSatisfy

import sbt._
import Keys._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.io.{File=>JavaFile}
import java.io.FileInputStream

object SatisfyPlugin extends sbt.Plugin {
  
   def hadoopConfDir : File = {
       if(sys.env.contains("HADOOP_CONF_DIR") ) {
          new File( sys.env("HADOOP_CONF_DIR"))   
       } else {
          if(sys.env.contains("HADOOP_HOME")) {
              new File( sys.env("HADOOP_HOME") + "/etc/hadoop")   
          } else {
             new File( "/usr/lib/hadoop/etc/hadoop" )
          }
       }
    }

    val HadoopResources = Set( "core-site.xml",
		  				"hdfs-site.xml",
   		  				"yarn-site.xml",
   		  				"mapred-site.xml")
  
    		  				     
  def hadoopConfiguration : org.apache.hadoop.conf.Configuration = {
       val hadoopConf = new org.apache.hadoop.conf.Configuration
       
       hadoopConf.set( "fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem" )
       hadoopConf.set( "fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem")
       val newCl = this.getClass().getClassLoader()
       hadoopConf.setClassLoader( newCl)
       
       val hadoopDir = hadoopConfDir 
        println(s"HADOOP Config Directory = $hadoopDir")
        if( hadoopDir != null && hadoopDir.exists  && hadoopDir.isDirectory) {
           HadoopResources.foreach( res => {
              val resFile = new File(hadoopDir.getPath + "/" + res)
              if(resFile.exists() ) {
                println(s" Adding resource ${resFile.getPath} ")
                hadoopConf.addResource( new FileInputStream(resFile))
              }
           } )
        } else {
          println(" Invalid Hadoop Config directory")
        }
    
       hadoopConf
  }
  
  object SatisfyKeys {
    lazy val hdfsURI = SettingKey[java.net.URI]("hdfs-uri", "The URI to upload to")
    lazy val basePath = SettingKey[String]("base-track-path", "The base HDFS upload path " )
    lazy val trackName = SettingKey[String]("track-name", "The name of the track uploaded " )
    lazy val trackProperties = SettingKey[String]("track-properties", "The track properties file to be uploaded " )
    lazy val trackVariant = SettingKey[String]("track-variant", "The name of the track variant " )
    lazy val trackUser = SettingKey[String]("track-user", "The name of the track user setting, if desired " )
    lazy val overwriteTrack = SettingKey[Boolean]("track-overwrite", "Flag to overwrite track if it exists" )
    
    lazy val trackPath = TaskKey[Path]("project-track-path", "The base path for a track" )
    lazy val uploadJarsPath = TaskKey[Path]("upload-jars-path", "The path which track jars are uploaded to" )
    lazy val uploadResourcePath = TaskKey[Path]("upload-resource-path", "The path which track resources are uploaded to" )

    lazy val uploadTrack = TaskKey[Unit]("upload", "Upload all files  to HDFS") 
    lazy val uploadProperties = TaskKey[Unit]("upload-properties", "Upload the satisfaction.properties file to HDFS")
    lazy val uploadPackageBin = TaskKey[Unit]("upload-package-bin", "Upload the target jar file to HDFS")
    lazy val uploadJars = TaskKey[Unit]("upload-jars", "Upload the dependency jar directory to HDFS")
    lazy val uploadResources = TaskKey[Unit]("upload-resources", "Upload the resource directory structure to HDFS")
    
    
  }
  import SatisfyKeys._
  
  def satisfySettings : Seq[Setting[_]] = Seq(
     hdfsURI := new java.net.URI("hdfs://dhdp2"),
     basePath := "/user/satisfaction/track" ,
     trackProperties := "conf/satisfaction.properties",
     trackName := "trackName",  /// XXX Get Track name from project name
     trackVariant := "",
     trackUser := "",
     overwriteTrack := false,
     
     trackPath <<= ( hdfsURI, basePath, trackName ,trackUser, trackVariant, version) map trackPathTask,
     uploadJarsPath <<= ( trackPath) map appendPathTask("lib"),
     uploadResourcePath <<= ( trackPath) map appendPathTask("resources"),
     
     uploadJars <<= ( hdfsURI, uploadJarsPath , (dependencyClasspath in Runtime), overwriteTrack, streams) map uploadFilesAttributed,
     
     uploadResources <<= ( hdfsURI, uploadResourcePath , resources in Runtime, overwriteTrack, streams) map uploadFiles,
     uploadProperties <<= ( hdfsURI, trackPath , trackProperties, overwriteTrack, streams) map uploadSingleFilename,
     uploadPackageBin <<= ( hdfsURI, uploadJarsPath , packageBin in Compile, overwriteTrack, streams) map uploadSingleFile,
     uploadPackageBin <<= uploadPackageBin.dependsOn(packageBin in Compile),
     
     uploadTrack <<= ( trackName, streams ) map uploadTask,
     uploadTrack <<= uploadTrack.dependsOn( uploadPackageBin ),
     uploadTrack <<= uploadTrack.dependsOn( uploadJars ),
     uploadTrack <<= uploadTrack.dependsOn( uploadResources ),
     uploadTrack <<= uploadTrack.dependsOn( uploadProperties )
  )
  
  override lazy val settings = satisfySettings
  
  def uploadTask( trackName: String, strms : TaskStreams ) = {
      strms.log.info(s"Uploading Satisfaction Track $trackName ") 
  }
  
  def appendPathTask(subDir : String)( trackPath : Path ) : Path = {
      new Path( trackPath.toString + "/" + subDir )   
  }
  
  
  def trackPathTask( hdfs: java.net.URI, base : String, trackName : String, user :String, variant : String, version: String ) : Path = {
    val sb = new StringBuilder
    sb.append( hdfs.toString)
    if (! base.startsWith( "/") ) {
       sb.append( "/")
    }
    sb.append( base)
    sb.append( "/")
    sb.append( trackName)
    if( user.length > 0 ) {
      sb.append("/")
      sb.append(user)
    }
    if( variant.length > 0 ) {
      sb.append("/")
      sb.append(variant)
    }
    sb.append("/version_")
    sb.append(version)
    
    new Path( sb.toString)
  }
  
  def uploadSingleFilename( hdfsURI : java.net.URI, destPath : Path,  filename : String , overwrite : Boolean, strms: TaskStreams) : Unit = {
    val singleFile = Seq(new java.io.File(filename))
    uploadFiles(hdfsURI, destPath, singleFile, overwrite, strms )
  }

  def uploadSingleFile( hdfsURI : java.net.URI, destPath : Path,  file:java.io.File , overwrite : Boolean, strms: TaskStreams) : Unit = {
    uploadFiles(hdfsURI, destPath, Seq(file), overwrite, strms )
  }
  
  def uploadFilesAttributed( hdfsURI : java.net.URI, destPath : Path,  srcFiles: Seq[Attributed[java.io.File]] , overwrite : Boolean, strms: TaskStreams) : Unit = {
    val unattributed = srcFiles.map( _.data  )
     uploadFiles(hdfsURI, destPath, unattributed, overwrite, strms )
  }
  
  
  def uploadFiles( hdfsURI : java.net.URI, destPath : Path,  srcFiles: Seq[java.io.File] , overwrite : Boolean, strms: TaskStreams) : Unit = {
    try {
    strms.log.info("Uploading to HDFS "+ hdfsURI.toString + " at path " + destPath.toString )
    val destFS = FileSystem.get(hdfsURI, hadoopConfiguration)
    if( ! destFS.exists(  destPath )) {
         strms.log.info(" Creating path " + destPath)
         destFS.mkdirs(destPath) 
     } else {
       if(overwrite) {
         strms.log(s"Overwriting existing track project path  $destPath")
         destFS.delete( destPath)
         destFS.mkdirs(destPath) 
       } else {
         strms.log(s" Track path $destPath already exists! Aborting !!")
         throw new RuntimeException(s" Track path $destPath already exists! Aborting !!")
       }
       
     }
     srcFiles.foreach( file => {
       if( file.isFile ) {
          strms.log.info(" Uploading File " + file.getPath)
          val outStream = destFS.create( new Path( destPath.toString + "/" + file.getName ))
          val inStream = new FileInputStream( file)
          IO.transfer(inStream,outStream) 
       } else if( file.isDirectory) {
         val dirContents = file.listFiles.toSeq
         uploadFiles( hdfsURI, destPath, dirContents, overwrite, strms) 
       }
     })
     
    } catch {
      case unexpected : Throwable =>
         strms.log.error( "Unexpected error " + unexpected)
         unexpected.printStackTrace
         unexpected.printStackTrace( System.out)
         throw unexpected
    }
  }

}
