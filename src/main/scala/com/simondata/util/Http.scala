package com.simondata.util

import java.io.{DataInputStream, DataOutputStream}
import java.net.{HttpURLConnection, URL}
import java.util.concurrent.TimeUnit

import play.api.libs.json.JsValue

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

class HttpHeaders(val map: Map[String, String] = Map.empty)

sealed abstract class HttpMethod(val value: String)
case object HttpGet extends HttpMethod("GET")
case object HttpPost extends HttpMethod("POST")

sealed trait HttpData
case class HttpBytes(data: Array[Byte]) extends HttpData
case class HttpJson(data: JsValue) extends HttpData
case class HttpString(data: String) extends HttpData

case class HttpResponse(status: Int, body: String)
case class HttpOptions(
  method: HttpMethod = HttpGet,
  headers: HttpHeaders = new HttpHeaders,
  data: Option[HttpData] = None,
  connectTimeout: Duration = Duration(15, TimeUnit.SECONDS),
  readTimeout: Duration = Duration(60, TimeUnit.SECONDS)
) {
  def withMethod(method: HttpMethod): HttpOptions = this.copy(method)
}

object Http {
  def get(url: String, options: HttpOptions = HttpOptions()): HttpResponse = request(url, options.withMethod(HttpGet))
  def post(url: String, options: HttpOptions = HttpOptions()): HttpResponse = request(url, options.withMethod(HttpPost))
  def request(
    url: String,
    options: HttpOptions
  ): HttpResponse = {
    val conn: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]

    // Set headers
    options.headers.map.foreach { case (key, value) =>
      conn.setRequestProperty(key, value)
    }

    conn.setRequestMethod(options.method.value)
    conn.setConnectTimeout(options.connectTimeout.toMillis.toInt)
    conn.setReadTimeout(options.readTimeout.toMillis.toInt)

    // Notify that we will read from the connection
    conn.setDoInput(true)

    options.data map { data =>
      // Notify that we will write to the connection
      conn.setDoOutput(true)

      val os = new DataOutputStream(conn.getOutputStream)
      data match {
        case HttpBytes(bytes) => os.write(bytes)
        case HttpJson(json) => os.write(json.toString.getBytes("utf-8"))
        case HttpString(text) => os.write(text.getBytes("utf-8"))
      }
      os.flush()
      os.close()
    }

    val is = new DataInputStream(conn.getInputStream)
    val content = new String(is.readAllBytes())
    val statusCode = conn.getResponseCode

    // clean up the connection
    conn.disconnect()

    HttpResponse(statusCode, content)
  }

  object async {
    def get(
      url: String,
      options: HttpOptions = HttpOptions()
    )(
      implicit ec: ExecutionContext
    ): Future[HttpResponse] = request(url, options.withMethod(HttpGet))

    def post(
      url: String,
      options: HttpOptions = HttpOptions()
    )(
      implicit ec: ExecutionContext
    ): Future[HttpResponse] = request(url, options.withMethod(HttpPost))

    def request(
     url: String,
     options: HttpOptions
    )(
      implicit ec: ExecutionContext
    ): Future[HttpResponse] = Future {
      Http.request(url, options)
    }
  }
}
