/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.trino

import com.simondata.util.{Config, Env, Slack, SlackSendError, Time}
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

sealed abstract class LogLevel(val name: String, val priority: Int)
case object DebugLevel extends LogLevel("TRACE", 10)
case object InfoLevel extends LogLevel("INFO", 20)
case object WarnLevel extends LogLevel("WARNING", 30)
case object ErrorLevel extends LogLevel("ERROR", 40)
case object FatalLevel extends LogLevel("FATAL", 50)

/**
 * Unified logger which supports multiple outputs (stdout and Slack in its initial implementation).
 *
 * @param identity the identity to which the log message relates
 * @param pluginContext the plugin which generated the log message
 */
class Logger(identity: AuthId)(implicit pluginContext: PluginContext) {
  private implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  // Convenience methods
  def trace(messageSupplier: Boolean => String): Unit = log(DebugLevel, messageSupplier)
  def info(messageSupplier: Boolean => String): Unit = log(InfoLevel, messageSupplier)
  def warn(messageSupplier: Boolean => String): Unit = log(WarnLevel, messageSupplier)
  def error(messageSupplier: Boolean => String): Unit = log(ErrorLevel, messageSupplier)
  def fatal(messageSupplier: Boolean => String): Unit = log(FatalLevel, messageSupplier)

  // More convenience
  def trace(message: String): Unit = log(DebugLevel, _ => message)
  def info(message: String): Unit = log(InfoLevel, _ => message)
  def warn(message: String): Unit = log(WarnLevel, _ => message)
  def error(message: String): Unit = log(ErrorLevel, _ => message)
  def fatal(message: String): Unit = log(FatalLevel, _ => message)

  def clusterName: String = Config.cluster.name
  def pluginName: String = pluginContext.name

  /**
   * This is the primary log function. It logs at the specified level, optionally forwarding
   * the output of the messageSupplier to Slack.
   *
   * @param level The priority level (LogLevel)
   * @param messageSupplier The message supplier function, which accepts a boolean to indicate whether
   *                        the output supports rich content (Slack, in this case)
   * @param sendToSlack An optional override to enforce whether or not the message is forwarded to slack
   *                    instead of relying on the level to decide (Warn or higher by default)
   * @param slackMessageColor The color of the attachment bar to the left of the Slack message
   * @param slackLevelEmoji The emoji to display to the right of the level indicator in Slack
   */
  def log(
    level: LogLevel,
    messageSupplier: Boolean => String,
    sendToSlack: Option[Boolean] = None,
    slackMessageColor: Option[String] = None,
    slackLevelEmoji: Option[String] = None
  ): Unit = {
    implicit val logLevel: LogLevel = level

    // Write to stdout
    println(jsonMessage(messageSupplier(false)))

    // Resolve whether a message should be delivered to Slack
    val sendSlackMessage = (sendToSlack, Config.nodeType.isCoordinator, level.priority > InfoLevel.priority) match {
      case (Some(true), true, _) => true
      case (None, true, true) => true
      case _ => false
    }

    if (sendSlackMessage) {
      Future {
        // Generate the Slack message
        slackMessage(messageSupplier(true), color = slackMessageColor, emoji = slackLevelEmoji)
      } flatMap {
        // Dispatch via webhook
        Slack.sendMessageAsync(_)
      } andThen {
        case Failure(e: SlackSendError) => println(s"Slack send failed: ${e.status} ${e.response}")
        case Failure(e) => {
          println("Unrecognized Slack error:")
          e.printStackTrace()
        }
      }
    }
  }

  def jsonMessage(message: String)(
    implicit level: LogLevel
  ): JsValue = {
    Json.obj(
      "timestamp" -> Time.iso,
      "cluster" -> clusterName,
      "identity" -> identity.toString,
      "level" -> level.name,
      "message" -> message
    )
  }

  /**
   * Wrap the Slack message in a consistent format. This generates a Slack message using blocks for
   * consistent structure and formatting. The message is in the form of an attachment, permitting
   * the colored left bar.
   *
   *
   * @param message the log message
   * @param color an optional custom color for the bar to the left of the message
   * @param emoji an optional custom emoji to display to the right of the timestamp and log level
   * @param fallbackMessage an optional fallback message, which defaults to the full log message
   * @param level the log level, which drives color and emoji if they were not customized
   *
   * @return a Play JSON object
   */
  def slackMessage(
    message: String,
    color: Option[String] = None,
    emoji: Option[String] = None,
    fallbackMessage: Option[String] = None
  )(
    implicit level: LogLevel
  ): JsObject = {
    val (defaultColor: String, defaultEmoji: String) = level match {
      case FatalLevel => ("#7E57C2", ":skull:")
      case ErrorLevel => ("#F44336", ":no_entry:")
      case WarnLevel => ("#EB984E", ":warning:")
      case InfoLevel => ("#888888", ":information_source:")
      case _ => ("#85C1E9", "")
    }

    val messageColor: String = color.getOrElse(defaultColor)
    val messageEmoji: String = emoji.getOrElse(defaultEmoji)

    val meta = s"${Time.iso} | _${level.name}_ ${messageEmoji}"
    val summary = s"[*${pluginName}* | cluster:`${clusterName}` | `${identity}`]"
    val fallback = fallbackMessage.getOrElse(message)

    Json.obj(
      "attachments" -> Json.arr(
        Json.obj(
          "color" -> messageColor,
          "fallback" -> fallback,
          "blocks" -> Json.arr(
            Json.obj(
              "type" -> "section",
              "text" -> Json.obj(
                "type" -> "mrkdwn",
                "text" -> summary
              )
            ),
            Json.obj(
              "type" -> "context",
              "elements" -> Json.arr(
                Json.obj(
                  "type" -> "mrkdwn",
                  "text" -> meta
                )
              )
            ),
            Json.obj(
              "type" -> "divider"
            ),
            Json.obj(
              "type" -> "section",
              "text" -> Json.obj(
                "type" -> "mrkdwn",
                "text" -> message
              )
            ),
            Json.obj(
              "type" -> "divider"
            )
          )
        )
      )
    )
  }
}

object Logger {
  def log(id: AuthId)(implicit pc: PluginContext): Logger = new Logger(id)
  def log(id: Namespace)(implicit pc: PluginContext): Logger = new Logger(AuthIdUser(id.name))
  def log(implicit pc: PluginContext): Logger = new Logger(AuthId.unknown)
}
