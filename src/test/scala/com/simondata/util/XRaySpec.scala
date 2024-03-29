/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.util

import com.simondata.UnitSpec

class XRaySpec extends UnitSpec {
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
