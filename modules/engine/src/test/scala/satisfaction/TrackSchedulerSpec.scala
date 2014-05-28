package com.klout
package satisfaction
package engine
package actors

/*
 * Tests for Scheduler
 */

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification // this is from Akka
import satisfaction.fs.LocalFileSystem

//@RunWith(classOf[JUnitRunner])
class TrackSchedulerSpec extends Specification {
 val mockFS = new LocalFileSystem
 
 val resourcePath = LocalFileSystem.currentDirectory / "modules" / "engine" / "src" /
            "test" / "resources";
   
}