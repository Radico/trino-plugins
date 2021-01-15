package com.simondata.util

import play.api.libs.json.{JsObject, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

case class SlackSendError(status: Int, response: Option[String]) extends Throwable

object Slack {
  private lazy val defaultSlackWebhook: Option[String] = Config.slackWebhook

  private def prepare(url: String, message: Either[String, JsObject]): (HttpHeaders, HttpData) = {
    val payload = message match {
      case Left(text) => HttpJson(Json.obj("text" -> text))
      case Right(json) => HttpJson(json)
    }
    val headers = new HttpHeaders(
      Map(
        "User-Agent" -> "trino-plugins",
        "Connection" -> "close",
        "Content-Type" -> "application/json",
        "Accept" -> "application/json, text/plain, */*",
      )
    )

    (headers, payload)
  }

  private def handleResponse(response: HttpResponse): Unit = {
    response match {
      case HttpResponse(status, _) if status < 400 => ()
      case HttpResponse(status, body) => throw SlackSendError(status, Option(body))
    }
  }

  /**
   * Send a message to Slack via a webhook synchronously.
   *
   * @param message the message to deliver to Slack
   * @param webhookUrl an optional webhook URL (if absent, and attempt will be made to fetch this from the environment)
   *
   * @return a Try indicating whether the send succeeded
   */
  def sendMessage(
    message: Either[String, JsObject],
    webhookUrl: Option[String] = defaultSlackWebhook
  ): Try[Unit] = webhookUrl match {
    case None => Failure(SlackSendError(0, Some("No webhook URL supplied")))
    case Some(url) => Try {
      val (headers, payload) = prepare(url, message)
      handleResponse(Http.post(url, HttpOptions(headers = headers, data = Some(payload))))
    }
  }

  /**
   * Send a message to Slack via a webhook asynchronously.
   *
   * @param message the message to deliver to Slack
   * @param webhookUrl an optional webhook URL (if absent, and attempt will be made to fetch this from the environment)
   *
   * @return a Future which will indicate whether the send succeeded
   */
  def sendMessageAsync(
    message: Either[String, JsObject],
    webhookUrl: Option[String] = defaultSlackWebhook
  )(
    implicit ec: ExecutionContext
  ): Future[Unit] = webhookUrl match {
    case None => Future.failed(SlackSendError(0, Some("No webhook URL supplied")))
    case Some(url) => {
      val (headers, payload) = prepare(url, message)
      Http.async.post(url, HttpOptions(headers = headers, data = Some(payload))) map {
        handleResponse(_)
      }
    }
  }

  // Aliases
  def sendMessage(message: String): Try[Unit] = sendMessage(Left(message))
  def sendMessage(message: JsObject): Try[Unit] = sendMessage(Right(message))
  def sendMessageAsync(message: String)(implicit ec: ExecutionContext): Future[Unit] = sendMessageAsync(Left(message))
  def sendMessageAsync(message: JsObject)(implicit ec: ExecutionContext): Future[Unit] = sendMessageAsync(Right(message))
}
