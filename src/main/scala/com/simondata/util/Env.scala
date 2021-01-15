package com.simondata.util

import scala.util.{Failure, Success, Try}

object Env extends ConfigSupplier {
  /**
   * Fetch an environment variable.
   *
   * @param key the environment variable to fetch
   *
   * @return an Option which is only populated if a non-empty value is found for the key
   */
  override def get(key: String): Option[String] = Try {
    Option(System.getenv(key))
  } match {
    case Failure(_) => None
    case Success(entry) => entry map { _.trim } filter { !_.isEmpty }
  }
}
