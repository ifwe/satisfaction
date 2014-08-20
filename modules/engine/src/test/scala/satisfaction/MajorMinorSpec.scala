package satisfaction
package track

import org.specs2.mutable._
import scala.concurrent.duration._
import org.joda.time.DateTime
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import scala.util.Success
import scala.util.Failure
import satisfaction.engine._
import fs._
import satisfaction.track.TrackFactory.MajorMinorPatch

/**
 *  Test that we're able to sort versions correctly
 */
class MajorMinorSpec extends Specification {
  
      "MajorMinorSpec" should {
      
        "Be able to parse a str" in {
          
          val mm = MajorMinorPatch("2.1.3")
          
          mm.majorVersion must_== 2
          mm.minorVersion must_== 1
          mm.patchNumber must_== 3
        }

        "Be able to parse a version_str" in {
          
          val mm = MajorMinorPatch("version_2.1.3")
          
          mm.majorVersion must_== 2
          mm.minorVersion must_== 1
          mm.patchNumber must_== 3
        }

        "Compare two  versions"  in {
          
          val version2 = MajorMinorPatch( "2.1.3")
          val version3 = MajorMinorPatch("3.0.1")
          
          val cp = version2.compare( version3)
          
          cp must be < 0
          
          version3 must be > version2
        }

        "Compare two Greater"  in {
          
          val version4 = MajorMinorPatch( "4.6.0")
          val version3 = MajorMinorPatch("3.0.1")
          
          val cp = version4.compare( version3)
          
          cp must be > 0
          
          version3 must be < version4
        }

        "Compare two equal"  in {
          
          val version2_1 = MajorMinorPatch("2.1.0")
          val version2_2 = MajorMinorPatch("2.1.0")
          
          val cp = version2_1.compare( version2_2)
          
          cp must_== 0
          
          version2_1  must_== version2_2
        }

        "handle only major minr"  in {
          
          val version4 = MajorMinorPatch( "4.6")
          val version3 = MajorMinorPatch("3")
          
          val cp = version4.compare( version3)
          
          cp must be > 0
          
          version3 must be < version4
        }

        "handle version greater than 10"  in {
          
          val version2_11 = MajorMinorPatch( "2.11.1")
          val version2_8 = MajorMinorPatch("2.8.3")
          
          val cp = version2_11.compare( version2_8)
          
          cp must be > 0
          
          version2_8 must be < version2_11
          version2_11 must be > version2_8

        }



     }
}