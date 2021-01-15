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

class Logger(identity: AuthId)(implicit pluginContext: PluginContext) {
  private implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  def trace(message: String): Unit = log(DebugLevel, message)
  def info(message: String): Unit = log(InfoLevel, message)
  def warn(message: String): Unit = log(WarnLevel, message)
  def error(message: String): Unit = log(ErrorLevel, message)
  def fatal(message: String): Unit = log(FatalLevel, message)

  def clusterName: String = Config.cluster.name
  def pluginName: String = pluginContext.name

  def log(
    level: LogLevel,
    message: String,
    sendToSlack: Option[Boolean] = None,
    slackMessageColor: Option[String] = None
  ): Unit = {
    implicit val logLevel: LogLevel = level

    val notifySlack = (sendToSlack, Config.nodeType.isCoordinator, level.priority > InfoLevel.priority) match {
      case (Some(true), true, _) => true
      case (None, true, true) => true
      case _ => false
    }

    println(jsonMessage(message))

    if (notifySlack) {
      Future {
        slackMessage(message, color=slackMessageColor)
      } flatMap {
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

  def slackMessage(
    message: String,
    color: Option[String] = None,
    fallbackMessage: Option[String] = None
  )(
    implicit level: LogLevel
  ): JsObject = {
    val (messageColor: String, messageEmoji: String) = (color, level) match {
      case (Some(c), _) => c
      case (None, FatalLevel) => ("#7E57C2", ":skull:")
      case (None, ErrorLevel) => ("#F44336", ":no_entry:")
      case (None, WarnLevel) => ("#EB984E", ":warning:")
      case (None, InfoLevel) => ("#888888", ":information_source:")
      case (None, _) => ("#85C1E9", "")
    }

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
