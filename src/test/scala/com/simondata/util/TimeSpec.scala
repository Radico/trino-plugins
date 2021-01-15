package com.simondata.util

import java.util.concurrent.TimeUnit

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.Duration

class TimeSpec extends AnyWordSpec with Matchers {
  "Time" when {
    "formatting durations" should {
      "display hours" in {
        assert(Time.human(Duration(1, TimeUnit.HOURS)) == "1h, 0m")
        assert(Time.human(Duration(65, TimeUnit.MINUTES)) == "1h, 5m")
        assert(Time.human(Duration(3600, TimeUnit.SECONDS)) == "1h, 0m")
      }

      "display minutes" in {
        assert(Time.human(Duration(1, TimeUnit.MINUTES)) == "1m, 0s")
        assert(Time.human(Duration(59, TimeUnit.MINUTES)) == "59m, 0s")
        assert(Time.human(Duration(3599, TimeUnit.SECONDS)) == "59m, 59s")
      }

      "display seconds" in {
        assert(Time.human(Duration(1, TimeUnit.SECONDS)) == "1.000s")
        assert(Time.human(Duration(1001, TimeUnit.MILLISECONDS)) == "1.001s")
        assert(Time.human(Duration(59999, TimeUnit.MILLISECONDS)) == "59.999s")
      }

      "display milliseconds" in {
        assert(Time.human(Duration(1, TimeUnit.MILLISECONDS)) == "1.000ms")
        assert(Time.human(Duration(1001, TimeUnit.MICROSECONDS)) == "1.001ms")
        assert(Time.human(Duration(59999, TimeUnit.MICROSECONDS)) == "59.999ms")
      }

      "display microseconds" in {
        assert(Time.human(Duration(1, TimeUnit.MICROSECONDS)) == "1.000us")
        assert(Time.human(Duration(1001, TimeUnit.NANOSECONDS)) == "1.001us")
        assert(Time.human(Duration(59999, TimeUnit.NANOSECONDS)) == "59.999us")
      }
    }
  }
}
