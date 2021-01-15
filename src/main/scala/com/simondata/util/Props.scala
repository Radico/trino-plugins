package com.simondata.util

import java.io.{FileNotFoundException, FileReader}
import java.util.Properties

import com.simondata.trino.Logger

import scala.util.{Failure, Success, Try}

class Props(props: Option[Properties]) extends ConfigSupplier {
  def get(key: String): Option[String] = props
      .flatMap { p => Option(p.getProperty(key, null)) }
      .map { _.trim}
      .filter { !_.isEmpty }
}

object Props {
  def read(file: String): Props = {
    Try {
      val props = new Properties
      val reader = new FileReader(file)
      props.load(reader)
      new Props(Some(props))
    } match {
      case Success(p) => p
      case Failure(e: FileNotFoundException) => {
        println(s"Supplemental config file not found: ${e}")
        new Props(None)
      }
      case Failure(e: Throwable) => {
        println(s"Error reading supplemental config file: ${e}")
        throw e
      }
    }
  }
}
