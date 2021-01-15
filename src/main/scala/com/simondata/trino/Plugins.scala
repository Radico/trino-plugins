package com.simondata.trino

import com.simondata.util.{ClassInfo, Config, Net, XRay}

import scala.util.{Failure, Success, Try}

object Plugins {
  private implicit val pc: PluginContext = AuthPlugin
  private var initialized = false

  def init(): Unit = synchronized {
    if (!initialized) {
      // Starburst has differed from the open source version in the past
      println("Inspecting SPI classes...")
      xrayClass("io.trino.spi.Plugin")
      xrayClass("io.trino.spi.security.SystemAccessControl")
      xrayClass("io.trino.spi.security.SystemAccessControlFactory")
      xrayClass("io.trino.spi.eventlistener.EventListener")
      xrayClass("io.trino.spi.eventlistener.EventListenerFactory")

      println("Initializing custom authenticator...")

      val idStr = Config.instanceId.getOrElse("non-ec2")
      val ipStr = Net.ips.mkString(" | ")
      val nodeStr = Config.nodeType.name
      val url = (Config.coordinatorIp, Config.coordinatorPort) match {
        case (Some(ip), Some(port)) => s"http://${ip}:${port}"
        case _ => "NO_URL"
      }

      Logger.log.info(s"Trino ${nodeStr} IPs: ${ipStr}")

      Logger.log.log(
        InfoLevel,
        s":online: Trino ${nodeStr} started.\nInstance ID: `${idStr}`\nURL: ${url}",
        sendToSlack = Some(Config.nodeType.isCoordinator)
      )

      initialized = true
    }
  }

  def xrayClass(className: String): Unit = {
    Try {
      XRay.reflectClassName(className)
    } match {
      case Success(classInfo: ClassInfo) => {
        println(s"XRAY[${className}] children:")
        classInfo.children.foreach { child => println(child.asJson) }

        println(s"XRAY[${className}] fields:")
        classInfo.fields.foreach { field => println(field.asJson) }

        println(s"XRAY[${className}] methods:")
        classInfo.methods.foreach { method => println(method.asJson) }

      }
      case Failure(error) => {
        println(s"XRAY[${className}] => Error inspecting class")
        error.fillInStackTrace().printStackTrace()
      }
    }
  }
}
