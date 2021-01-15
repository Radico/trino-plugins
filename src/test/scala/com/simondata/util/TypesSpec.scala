package com.simondata.util

import java.util.Optional

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TypesSpec extends AnyWordSpec with Matchers {
  "Types" when {
    "translating options" should {
      "convert a Java Optional to a Scala Option" in {
        val maybeJ: Optional[Boolean] = Optional.of(true)
        val maybe: Option[Boolean] = Types.toOption(maybeJ)
        maybe match {
          case Some(true) => assert(true)
          case _ => assert(false, "Should have been Some(true)")
        }

        val maybeNotJ: Optional[Boolean] = Optional.empty
        val maybeNot: Option[Boolean] = Types.toOption(maybeNotJ)
        maybeNot match {
          case Some(true) => assert(false, "Should have been None")
          case _ => assert(true)
        }
      }

      "convert a Scala Option to a Java Optional" in {
        val maybeS: Option[Boolean] = Some(true)
        val maybe: Optional[Boolean] = Types.toOptional(maybeS)
        assert(maybe.isPresent)
        assert(maybe.get == true)

        val maybeNotS: Option[Boolean] = None
        val maybeNot: Optional[Boolean] = Types.toOptional(maybeNotS)
        assert(maybeNot.isEmpty)
      }
    }
  }
}
