package com.klout
package satisfaction
package engine
package actors


import org.specs2.mutable._

class HelloWorldSpec extends Specification {
  "The 'Cheese' string" should {
    "contain 6 characters" in {
      "Cheese" must have size(6)
    }
    
  }
}