package com.klout.satisfaction

import scalaxb._
import org.specs2.mutable._
import java.net.URLClassLoader
import java.lang.reflect.Method
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.conf.Configuration
import java.util.Properties

class ClassLoaderSpec extends Specification {

    "ClassLoader" should {
      /**
        "check if Load Class" in {
           val className=  "com.klout.platform.protos.Topics"
             
             val classLoader = this.getClass.getClassLoader
             
             val newClazz = classLoader.loadClass( className)
             
             
          
             println(" New Class is " + newClazz.getName)
         
        }
        "check if Load inner Class" in {
           val className=  "com.klout.platform.protos.Topics$FactContent"
             
             val classLoader = this.getClass.getClassLoader
             
             val newClazz = classLoader.loadClass( className)
             
             
          
             println(" New Inner Class is " + newClazz.getName)
         
        }
        * 
        */
      
      def addJar( jarUrl : String ) = {
                val currentLoader = this.getClass.getClassLoader
                if( currentLoader.isInstanceOf[URLClassLoader]) {
                  val currentURLLoader = currentLoader.asInstanceOf[URLClassLoader]
                  val method : Method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[java.net.URL])
                  method.setAccessible(true)
                  println(s" Adding to current classpath $jarUrl")
                  method.invoke( currentURLLoader, new java.net.URL(jarUrl))
                } else {
                   println(s" Current classloader is of type ${currentLoader.getClass} ; Cannot append classpath !!!") 
                }
                println(s" Adding to current classpath $jarUrl")
        
      }
      
      "check if add to classloader " in {
        
        
           val jarUrl = "file:///Users/jeromebanks/NewGit/satisfaction/auxlib/platform-protos-0.91.6.jar"
             
           val jar2Url = "file:///Users/jeromebanks/NewGit/satisfaction/auxlib/ecoadapters-0.5.2.jar"
             addJar( jarUrl)
             addJar( jar2Url)
              val className=  "com.klout.platform.protos.Topics$FactContent"
             
             val classLoader = this.getClass.getClassLoader
             
             ///val newClazz = classLoader.loadClass( className)
             
             
          
             ///println(" New Inner Class is " + newClazz.getName)
             ///
             
             
             val className2 = "com.inadco.ecoadapters.hive.ProtoSerDe"
         
             val newClazz2 = classLoader.loadClass( className2)
             println(" New Inner Class is " + newClazz2.getName)
             
             val protoObj = newClazz2.newInstance.asInstanceOf[SerDe]
             println( " ProtoSerde is " + protoObj)
             
             val conf = new Configuration
             val props = new Properties
             props.put( "messageClass", "com.klout.platform.protos.Topics$FactContent")
             protoObj.initialize( conf, props)
             
             println( " PRoto initialized !!!")
           
        
      }
      
      

  

  
    }

}