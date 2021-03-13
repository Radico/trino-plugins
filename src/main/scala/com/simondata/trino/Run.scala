/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.trino

import java.util.concurrent.TimeUnit

import com.simondata.util.{Config, Env, Http, HttpJson, HttpOptions, HttpResponse, Net, Slack, Time, XRay}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

/**
 * Convenience object for running manual tests against various utilities.
 */
object Run extends App {
  private implicit val pc: PluginContext = LocalPlugin
  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  val id = AuthIdUser("admin")
  val namespace = Namespace("default")

  val logger = Logger.log(namespace)(PluginContext.forName("trino-events"))

  def xrayClass(className: String): Unit = {
    val classInfo = XRay.reflectClassName(className)
    val json = classInfo.asJson.toString
    println(json)
  }

  def runXray(): Unit = {
    xrayClass("io.trino.spi.Plugin")
    xrayClass("io.trino.spi.security.EventListener")
    xrayClass("io.trino.spi.security.EventListenerFactory")
    xrayClass("io.trino.spi.security.SystemAccessControl")
    xrayClass("io.trino.spi.security.SystemAccessControlFactory")
  }

  def runHttp(): Unit = {
    val url = Env.get("HTTP_TEST_URL").getOrElse("http://localhost:8080")
    val body: JsValue = Json.obj(
      "message" -> "howdy"
    )
    Http.post(url, HttpOptions(data = Some(HttpJson(body)))) match {
      case HttpResponse(status, body) => {
        println(s"[$status] $body")
      }
    }
  }

  def runSlack(): Unit = {
    Await.ready(Slack.sendMessageAsync("Hello from trino-plugins"), Duration(30, TimeUnit.SECONDS))
  }

  def runSlack2(): Unit = {
    logger.warn("Testing a warning-level message")
    logger.error(
      """Testing an error-level message
        |with two lines?""".stripMargin)
    logger.fatal("Testing a fatal message\n...with two lines?")

    logger.log(InfoLevel, _ => "Sleeping to allow time for Slack message deliveries...", sendToSlack = Some(true))

    // Wait for slack to send all messages
    Thread.sleep(3000)

    logger.info("Done.")
  }

  def runTime(): Unit = {
    val time = Time.now
    println(s"The time is: ${Time.iso}")
  }

  def runLogger(): Unit = {
    Logger.log(id).info("Some info ...")

    val idStr = Config.instanceId.getOrElse("non-ec2")
    val ipStr = Net.ips.mkString(", ")

    Logger.log(id).warn(s"System[${Config.nodeType}]: ${idStr} => ${ipStr}")

    // Give some time for Slack send
    Thread.sleep(2000)
  }

  //runXray()
  //runHttp()
  //runSlack()
  runSlack2()
  //runAes()
  //runTime()
  //runLogger()
}
