package com.simondata.util

import java.security.SecureRandom

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class KeyNotFoundException(message: String) extends Exception(message)

object Aes {
  private val IV_LENGTH = 16

  private lazy val randomSource = SecureRandom.getInstanceStrong
  def randomBytes(byteCount: Int): Array[Byte] = randomSource.generateSeed(byteCount)

  /**
   * Decrypt cipher text with the supplied key and optional initialization vector (IV).
   * If the IV is not supplied, the first 16 bytes of the cipher text is used as the IV.
   *
   * @param cipherText the encrypted data
   * @param key the encryption key
   * @param iv optional initialization vector
   *
   * @return a Try which, on success, will contain the plain text
   */
  def decrypt(
    cipherText: Array[Byte], key: Array[Byte], iv: Option[Array[Byte]]
  ): Try[Array[Byte]] = Try {
    val (ivData: Array[Byte], ctData: Array[Byte]) = iv match {
      case Some(iv) => (iv, cipherText)
      case None => (cipherText.slice(0, IV_LENGTH), cipherText.slice(IV_LENGTH, cipherText.length))
    }

    val ivSpec = new IvParameterSpec(ivData)
    val keySpec = new SecretKeySpec(key, "AES")
    val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec)
    val plainText = cipher.doFinal(ctData)

    plainText
  }

  /**
   * Encrypt plain text using the supplied encryption key and optional initialization vector (IV).
   * If the IV is not supplied, it will be randomnly generated and prepended to the ciphertext.
   *
   * @param plainText the plain text data to encrypt
   * @param key the encryption key
   * @param iv optional initialization vector
   *
   * @return a Try which, on success, will contain the cipher text
   */
  def encrypt(
    plainText: Array[Byte], key: Array[Byte], iv: Option[Array[Byte]]
  ): Try[Array[Byte]] = Try {
    val (prependIv: Boolean, ivData: Array[Byte]) = iv match {
      case Some(iv) => (false, iv)
      case None => (true, randomBytes(IV_LENGTH))
    }

    val ivSpec = new IvParameterSpec(ivData)
    val keySpec = new SecretKeySpec(key, "AES")
    val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec)
    val cipherText = cipher.doFinal(plainText)

    prependIv match {
      case true => ivData ++ cipherText
      case false => cipherText
    }
  }
}
