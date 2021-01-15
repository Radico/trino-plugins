package com.simondata.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class XRaySpec extends AnyWordSpec with Matchers {
  def inner(caller: Boolean): Option[String] = if (caller) XRay.getCallerName() else XRay.getMethodName()
  def outer(caller: Boolean): Option[String] = inner(caller)

  "XRay" when {
    "fetching method name info" should {
      "be able to fetch the current method's name" in {
        assert(outer(false) == Some("inner"))
      }

      "be able to fetch the calling method's name" in {
        assert(outer(true)== Some("outer"))
      }
    }
  }
}
