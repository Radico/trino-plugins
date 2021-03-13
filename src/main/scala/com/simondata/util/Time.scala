/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.util

import java.time
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration
import scala.util.Success

object Time {
  private val SCHEDULER = new ScheduledThreadPoolExecutor(2)
  private val ISO_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private val ZONE_UTC: ZoneId = ZoneId.of("UTC")

  def now: ZonedDateTime = ZonedDateTime.now(ZONE_UTC)
  def iso: String = now.format(ISO_FORMAT)
  def toIso(timestamp: Instant): String = timestamp.atZone(ZONE_UTC).format(ISO_FORMAT)

  def zeroPad(value: Int): String = value match {
    case v if (v < 10) => s"00${v}"
    case v if (v < 100) => s"0${v}"
    case v => s"${v}"
  }

  def human(duration: Duration): String = human(time.Duration.ofNanos(duration.toNanos))
  def human(duration: java.time.Duration): String = (duration.getSeconds, duration.getNano) match {
    case (seconds, _) if (seconds >= 3600) => s"${seconds / 60 / 60}h, ${seconds / 60 % 60}m"
    case (seconds, _) if (seconds >= 60) => s"${seconds / 60}m, ${seconds % 60}s"
    case (seconds, nanos) if (seconds > 0) => s"${seconds}.${zeroPad(nanos / 1000000)}s"
    case (_, nanos) if (nanos >= 1000000) => s"${nanos / 1000 / 1000}.${zeroPad(nanos / 1000 % 1000)}ms"
    case (_, nanos) => s"${nanos / 1000}.${zeroPad(nanos % 1000)}us"
  }

  def sleep(duration: Duration)(implicit ec: ExecutionContext): Future[Unit] = {
    val promise = Promise[Unit]()
    val action = new Runnable() {
      def run(): Unit = {
        promise.complete(Success(()))
      }
    }
    SCHEDULER.schedule(action, duration.toMillis, TimeUnit.SECONDS)
    promise.future
  }
}
