/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.{JsObject, JsString, Json}

class ConfigSupplierSpec extends AnyWordSpec with Matchers {
  "ConfigSupplier" when {
    "fetching as strings" should {
      "get the a value if present" in {
        assert(ConfigSupplier.of(Map("key" -> "value")).getString("key").contains("value"))
      }

      "get nothing if value is empty" in {
        assert(ConfigSupplier.of(Map("key" -> " ")).getString("key").isEmpty)
        assert(ConfigSupplier.of(Map("key" -> "")).getString("key").isEmpty)
      }

      "get nothing if value is absent" in {
        assert(ConfigSupplier.of(Map.empty[String, String]).getString("key").isEmpty)
      }
    }

    "transforming from strings" should {
      "get a valid int on convert" in {
        assert(ConfigSupplier.of(Map("key" -> "0")).getAs("key")(_.toIntOption).contains(0))
        assert(ConfigSupplier.of(Map("key" -> "1")).getAs("key")(_.toIntOption).contains(1))
        assert(ConfigSupplier.of(Map("key" -> "-1")).getAs("key")(_.toIntOption).contains(-1))
      }

      "get a valid boolean on convert" in {
        assert(ConfigSupplier.of(Map("key" -> "true")).getAs("key")(_.toBooleanOption).contains(true))
        assert(ConfigSupplier.of(Map("key" -> "false")).getAs("key")(_.toBooleanOption).contains(false))
      }

      "get a valid json on convert" in {
        assert(
          ConfigSupplier.of(Map("key" -> """{"msg":"ack"}""")).getAs("key")(v => Option(Json.parse(v))) match {
            case Some(JsObject(obj)) => obj.get("msg") match {
              case Some(JsString("ack")) => true
              case _ => false
            }
            case _ => false
          }
        )
      }

      "get a valid boolean from a toggle input" in {
        assert(ConfigSupplier.of(Map("key" -> "true")).getToggle("key") == Some(true))
        assert(ConfigSupplier.of(Map("key" -> "yes")).getToggle("key") == Some(true))
        assert(ConfigSupplier.of(Map("key" -> "on")).getToggle("key") == Some(true))
        assert(ConfigSupplier.of(Map("key" -> "1")).getToggle("key") == Some(true))

        assert(ConfigSupplier.of(Map("key" -> "false")).getToggle("key") == Some(false))
        assert(ConfigSupplier.of(Map("key" -> "no")).getToggle("key") == Some(false))
        assert(ConfigSupplier.of(Map("key" -> "off")).getToggle("key") == Some(false))
        assert(ConfigSupplier.of(Map("key" -> "0")).getToggle("key") == Some(false))
      }

      "get nothing on failed int conversion" in {
        assert(ConfigSupplier.of(Map("key" -> "a")).getAs("key")(_.toIntOption).isEmpty)
        assert(ConfigSupplier.of(Map("key" -> "a")).getInt("key").isEmpty)
      }

      "get nothing on failed boolean conversion" in {
        assert(ConfigSupplier.of(Map("key" -> "a")).getAs("key")(_.toBooleanOption).isEmpty)
      }

      "get nothing on failed json conversion" in {
        assert(ConfigSupplier.of(Map("key" -> """}{""")).getAs("key")(v => Option(Json.parse(v))).isEmpty)
      }

      "get nothing on failed toggle conversion" in {
        assert(ConfigSupplier.of(Map("key" -> "meh")).getToggle("key").isEmpty)
        assert(ConfigSupplier.of(Map("yek" -> "meh")).getToggle("key").isEmpty)
      }
    }
  }
}
