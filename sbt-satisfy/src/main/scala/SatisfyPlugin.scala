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
        if( hadoopDir != null && hadoopDir.exists  && hadoopDir.isDirectory) {
           HadoopResources.foreach( res => {
              val resFile = new File(hadoopDir.getPath + "/" + res)
              if(resFile.exists() ) {
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

    /// XXX FIXME
    lazy val uploadExcludes = SettingKey[Seq[ModuleID]]("upload-excludes", "List of modules to exclude from uploading to HDFS" )
    
    lazy val uploadDependencies = TaskKey[Classpath]("upload-dependencies", "The classpath of the dependent jars to be uploaded to HDFS" )
    
    lazy val trackPath = TaskKey[Path]("track-path", "The base path for a track" )
    lazy val uploadJarsPath = TaskKey[Path]("upload-jars-path", "The path which track jars are uploaded to" )
    lazy val uploadResourcePath = TaskKey[Path]("upload-resource-path", "The path which track resources are uploaded to" )

    lazy val uploadTrack = TaskKey[Unit]("upload", "Upload all files  to HDFS") 
    lazy val uploadProperties = TaskKey[Unit]("upload-properties", "Upload the satisfaction.properties file to HDFS")
    lazy val uploadJars = TaskKey[Unit]("upload-jars", "Upload the dependency jar directory to HDFS")
    lazy val uploadPackage = TaskKey[Unit]("upload-package", "Upload the built package to HDFS")
    lazy val uploadResources = TaskKey[Unit]("upload-resources", "Upload the resource directory structure to HDFS")
    lazy val cleanUpload = TaskKey[Unit]("clean-upload", "Clean up the upload directory on HDFS")
    
    
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
     
     
     /// Exclude 
     /// XXX JDB filter these modules out of uploaded jars ...
     uploadExcludes := Seq(  ( "com.klout.satisfaction" %% "satisfaction-core" % "*" ) ,
            ( "com.klout.satisfaction" %% "satisfaction-engine" % "*" ),
            ( "com.klout.satisfaction" %% "satisfaction-hadoop" % "*" ),
            ( "org.apache.hadoop" %% "*" % "*" )
       ),

     uploadDependencies <<= dependencyClasspath in Runtime,
     
     uploadJars <<= ( hdfsURI, uploadJarsPath , uploadDependencies,  streams) map uploadFilesAttributed,
     
     uploadResources <<= ( hdfsURI, uploadResourcePath , resources in Runtime,  streams) map uploadFiles,
     uploadProperties <<= ( hdfsURI, trackPath , trackProperties, streams) map uploadSingleFilename,
     uploadPackage <<= ( hdfsURI, uploadJarsPath, (packageBin in Compile), streams) map uploadSingleFile,
     
     cleanUpload <<= ( hdfsURI, trackPath , overwriteTrack , streams) map cleanUploadPath,
     
     uploadTrack <<= (( trackName, streams ) map uploadTask ) 
     	   dependsOn uploadProperties
           dependsOn uploadJars 
           dependsOn uploadPackage
           dependsOn uploadResources
           dependsOn cleanUpload
           dependsOn (packageBin in Compile)
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
  
  def uploadSingleFilename( hdfsURI : java.net.URI, destPath : Path,  filename : String,  strms: TaskStreams) : Unit = {
    val singleFile = Seq(new java.io.File(filename))
    uploadFiles(hdfsURI, destPath, singleFile, strms )
  }

  def uploadSingleFile( hdfsURI : java.net.URI, destPath : Path,  file:java.io.File, strms: TaskStreams) : Unit = {
    uploadFiles(hdfsURI, destPath, Seq(file), strms )
  }
  

  def uploadFilesAttributed( hdfsURI : java.net.URI, destPath : Path,  srcFiles: Seq[Attributed[java.io.File]], strms: TaskStreams) : Unit = {
    val unattributed = srcFiles.map( _.data  )
     uploadFiles(hdfsURI, destPath, unattributed, strms )
  }
  
  def cleanUploadPath( hdfsURI : java.net.URI, destPath : Path, overwrite : Boolean, strms : TaskStreams ) : Unit = {
    strms.log.info("Cleaning HDFS "+ hdfsURI.toString + " at path " + destPath.toString )
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
  }
  
  def uploadFiles( hdfsURI : java.net.URI, destPath : Path,  srcFiles: Seq[java.io.File] , strms: TaskStreams) : Unit = {
    try {
    strms.log.info("Uploading to HDFS "+ hdfsURI.toString + " at path " + destPath.toString )
    val destFS = FileSystem.get(hdfsURI, hadoopConfiguration)
     srcFiles.foreach( file => {
       if( file.isFile ) {
          strms.log.info(s" Uploading File ${file.getPath} to destination path ${destPath}." )
          val outStream = destFS.create( new Path( destPath.toString + "/" + file.getName ))
          val inStream = new FileInputStream( file)
          
          IO.transfer(inStream,outStream) 
          
          outStream.close
          inStream.close
       } else if( file.isDirectory) {
         val dirContents = file.listFiles.toSeq
         uploadFiles( hdfsURI, destPath, dirContents, strms) 
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
