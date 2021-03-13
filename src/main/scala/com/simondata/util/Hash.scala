/**
 * Copyright 2020-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.util

import java.security.MessageDigest
import java.util.Base64

import org.apache.commons.codec.binary.Hex

case class Digest(bytes: Array[Byte]) {
  val hex: String = Hex.encodeHexString(bytes)
  val b64: String = Base64.getEncoder.encodeToString(bytes)
}

object Hash {
  def hash(content: Array[Byte], algorithm: String): Digest = Digest(MessageDigest.getInstance(algorithm).digest(content))
  def hash(content: String, algorithm: String): Digest = hash(content.getBytes("UTF-8"), algorithm)

  def md5(content: Array[Byte]): Digest = hash(content, "MD5")
  def md5(text: String): Digest = hash(text, "MD5")

  def sha1(content: Array[Byte]): Digest = hash(content, "SHA1")
  def sha1(text: String): Digest = hash(text, "SHA1")

  def sha256(content: Array[Byte]): Digest = hash(content, "SHA-256")
  def sha256(text: String): Digest = hash(text, "SHA-256")

  def sha512(content: Array[Byte]): Digest = hash(content, "SHA-512")
  def sha512(text: String): Digest = hash(text, "SHA-512")
}
